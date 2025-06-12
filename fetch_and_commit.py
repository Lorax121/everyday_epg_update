import os
import sys
import requests
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

SOURCES_FILE = 'sources.txt'
DATA_DIR = Path('data')
README_FILE = 'README.md'
MAX_WORKERS = 8
CHUNK_SIZE = 16 * 1024 

RAW_BASE = "https://github.com/Lorax121/everyday_epg_update/raw/main/data/"


def read_sources():
    entries = []
    with open(SOURCES_FILE, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = [p.strip() for p in line.split('|', 1)]
            url = parts[0]
            desc = parts[1] if len(parts) > 1 else ''
            entries.append({'url': url, 'desc': desc})
    if not entries:
        print("Error: нет ни одной ссылки в sources.txt!", file=sys.stderr)
        sys.exit(1)
    return entries


def clear_data_dir():
    if DATA_DIR.exists():
        for f in DATA_DIR.iterdir():
            f.unlink()
    else:
        DATA_DIR.mkdir(parents=True)


def detect_extension(file_path, url):
    with open(file_path, 'rb') as f:
        sig = f.read(4)
    # GZIP header: 1f 8b
    if sig[:2] == b"\x1f\x8b":
        return '.xml.gz'
    # XML: starts with '<?xml'
    if sig.startswith(b'<?xm'):
        return '.xml'
    # fallback: из URL
    suffixes = Path(urlparse(url).path).suffixes
    if suffixes:
        return ''.join(suffixes)
    return ''


def download_one(entry, raw_prefix):
    url = entry['url']
    temp = DATA_DIR / ("tmp_" + os.urandom(4).hex())
    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(temp, 'wb') as f:
                for chunk in r.iter_content(CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
        ext = detect_extension(temp, url)
        base = Path(urlparse(url).path).stem or "file"
        filename = f"{base}{ext}"
        target = DATA_DIR / filename
        temp.rename(target)

        size_mb = round(target.stat().st_size / (1024*1024), 2)
        return {
            'url': url,
            'desc': entry['desc'],
            'filename': filename,
            'raw_url': raw_prefix + filename,
            'size_mb': size_mb,
            'error': None
        }
    except Exception as e:
        if temp.exists():
            temp.unlink()
        return {'url': url, 'desc': entry['desc'], 'error': str(e)}


def update_readme(results):
    today = datetime.utcnow().date().isoformat()
    lines = [f"# Обновлено: {today}", ""]
    for idx, r in enumerate(results, 1):
        if r.get('error'):
            lines.append(f"{idx}. {r['url']} — {r['desc']} (ошибка: {r['error']})")
        else:
            lines.append(f"{idx}. {r['raw_url']} — {r['desc']} ({r['size_mb']} MB)")
    with open(README_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines) + "\n")
    print(f"README.md обновлён ({len(results)} записей)")


def main():
    repo = os.getenv('GITHUB_REPOSITORY', 'owner/repo')
    owner, repo_name = repo.split('/')
    raw_prefix = RAW_BASE.format(owner=owner, repo=repo_name)

    entries = read_sources()
    clear_data_dir()

    results = []
    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        futures = {ex.submit(download_one, e, raw_prefix): e for e in entries}
        for f in as_completed(futures):
            results.append(f.result())
    url_to_res = {r['url']: r for r in results}
    ordered = [url_to_res[e['url']] for e in entries]

    update_readme(ordered)


if __name__ == '__main__':
    main()
