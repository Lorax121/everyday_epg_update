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
CHUNK_SIZE = 16 * 1024  # 16KB
REPO_BASE_URL = "https://github.com/Lorax121/everyday_epg_update/raw/main/data/"


def read_sources():
    entries = []
    try:
        with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = [p.strip() for p in line.split('|', 1)]
                url = parts[0]
                desc = parts[1] if len(parts) > 1 else ''
                entries.append({
                    'url': url,
                    'desc': desc
                })
    except FileNotFoundError:
        print(f"Error: File {SOURCES_FILE} not found!", file=sys.stderr)
        sys.exit(1)
    return entries


def clear_data_dir():
    try:
        if DATA_DIR.exists():
            for f in DATA_DIR.iterdir():
                try:
                    f.unlink()
                except OSError as e:
                    print(f"Warning: Could not delete {f}: {e}", file=sys.stderr)
        else:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"Error: Could not prepare data directory: {e}", file=sys.stderr)
        sys.exit(1)


def get_safe_filename(url, local_path):
    # Получаем базовое имя из URL
    path = urlparse(url).path
    base_name = Path(path).name
    if not base_name or '.' not in base_name:
        base_name = "file"
    base_name = base_name.split('.')[0]

    ext = ''.join(local_path.suffixes)  # Берём все суффиксы (.tar.gz и т.д.)
    
    return f"{base_name}{ext.lower()}" 


def download(entry):
    url = entry['url']
    temp_path = DATA_DIR / "temp_download_file"
    
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            
            with open(temp_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)

            final_name = get_safe_filename(url, temp_path)
            final_path = DATA_DIR / final_name

            temp_path.rename(final_path)
            
            return ({
                **entry,
                'filename': final_name,
                'download_url': f"{REPO_BASE_URL}{final_name}"
            }, True, None)
    except Exception as e:
        if temp_path.exists():
            temp_path.unlink()
        return (entry, False, str(e))


def update_readme(entries):
    today = datetime.utcnow().date().isoformat()
    header = f"# Обновлено: {today}\n\n"
    
    lines = []
    for idx, e in enumerate(entries, 1):
        if 'filename' not in e:
            lines.append(f"{idx}. {e['url']} — {e['desc']} (download failed)")
            continue
        
        name = e['filename']
        desc = e['desc'] or name
        download_url = e['download_url']
        path = DATA_DIR / name
        
        try:
            size_bytes = path.stat().st_size
            size_mb = round(size_bytes / (1024 * 1024), 2)
            lines.append(f"{idx}. {download_url} — {desc} ({size_mb} MB)")
        except OSError:
            lines.append(f"{idx}. {download_url} — {desc} (file missing)")
    
    content = header + '\n'.join(lines) + '\n'
    
    try:
        with open(README_FILE, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Updated {README_FILE} with date {today} and {len(entries)} links")
    except IOError as e:
        print(f"Error writing to {README_FILE}: {e}", file=sys.stderr)


def main():
    repo_url = os.getenv('GITHUB_REPOSITORY', 'owner/repo')
    owner, repo = repo_url.split('/')
    global REPO_BASE_URL
    REPO_BASE_URL = REPO_BASE_URL.format(owner=owner, repo=repo)
    
    entries = read_sources()
    clear_data_dir()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download, e) for e in entries]
        
        has_errors = False
        updated_entries = []
        for future in as_completed(futures):
            entry, success, error = future.result()
            if success:
                print(f"✓ Downloaded {entry['url']} as {entry['filename']}")
                updated_entries.append(entry)
            else:
                print(f"✗ Error downloading {entry['url']}: {error}", file=sys.stderr)
                updated_entries.append(entry)
                has_errors = True
        
        if has_errors:
            print("\nSome downloads failed!", file=sys.stderr)

    update_readme(updated_entries)
    print("\nAll operations completed!")


if __name__ == '__main__':
    main()
