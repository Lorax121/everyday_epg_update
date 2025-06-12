import os
import sys
import requests
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Конфигурация
SOURCES_FILE = 'sources.txt'
DATA_DIR = Path('data')
README_FILE = 'README.md'
MAX_WORKERS = 8         
CHUNK_SIZE = 16 * 1024 


def read_sources():
    entries = []
    with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = [p.strip() for p in line.split('|', 1)]
            url = parts[0]
            desc = parts[1] if len(parts) > 1 else ''
            filename = Path(urlparse(url).path).name
            entries.append({'url': url, 'desc': desc, 'filename': filename})
    return entries


def clear_data_dir():
    if DATA_DIR.exists():
        for f in DATA_DIR.iterdir():
            f.unlink()
    else:
        DATA_DIR.mkdir(parents=True)


def download(entry):
    url = entry['url']
    local_path = DATA_DIR / entry['filename']
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(CHUNK_SIZE):
                    f.write(chunk)
        return (entry, True, None)
    except Exception as e:
        return (entry, False, str(e))


def update_readme(entries):
    today = datetime.utcnow().date().isoformat()
    header = f"Обновлено: {today}"

"
    lines = []
    for idx, e in enumerate(entries, 1):
        name = e['filename']
        desc = e['desc'] or name
        path = DATA_DIR / name
        size_mb = round(path.stat().st_size / (1024 * 1024), 2)
        link = f"[{name}]({DATA_DIR}/{name})"
        lines.append(f"{idx}) {link} → {desc} [{size_mb}мб]")
    content = header + '
'.join(lines) + '
'
    with open(README_FILE, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"Updated {README_FILE} with date {today} and {len(entries)} links")


def main():
    entries = read_sources()
    clear_data_dir()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download, e) for e in entries]
        for future in as_completed(futures):
            entry, success, error = future.result()
            if success:
                print(f"Downloaded {entry['url']}")
            else:
                print(f"Error downloading {entry['url']}: {error}", file=sys.stderr)
                sys.exit(1)

    update_readme(entries)

if __name__ == '__main__':
    main()
