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
CHUNK_SIZE = 16 * 1024  # 16KB


def read_sources():
    """Читает файл с источниками и возвращает список записей."""
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
                filename = Path(urlparse(url).path).name
                if not filename:
                    filename = f"file_{len(entries) + 1}.bin"
                entries.append({
                    'url': url,
                    'desc': desc,
                    'filename': filename
                })
    except FileNotFoundError:
        print(f"Error: File {SOURCES_FILE} not found!", file=sys.stderr)
        sys.exit(1)
    return entries


def clear_data_dir():
    """Очищает директорию с данными или создает новую."""
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


def download(entry):
    """Скачивает файл по URL из записи."""
    url = entry['url']
    local_path = DATA_DIR / entry['filename']
    
    try:
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    if chunk:  # фильтр keep-alive chunks
                        f.write(chunk)
        return (entry, True, None)
    except Exception as e:
        return (entry, False, str(e))


def update_readme(entries):
    """Обновляет README файл со списком скачанных файлов."""
    today = datetime.utcnow().date().isoformat()
    header = f"# Обновлено: {today}\n\n"
    
    lines = []
    for idx, e in enumerate(entries, 1):
        name = e['filename']
        desc = e['desc'] or name
        path = DATA_DIR / name
        
        try:
            size_bytes = path.stat().st_size
            size_mb = round(size_bytes / (1024 * 1024), 2)
            link = f"[{name}]({DATA_DIR.name}/{name})"
            lines.append(f"{idx}. {link} — {desc} ({size_mb} MB)")
        except OSError:
            lines.append(f"{idx}. {name} — {desc} (file missing)")
    
    content = header + '\n'.join(lines) + '\n'
    
    try:
        with open(README_FILE, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Updated {README_FILE} with date {today} and {len(entries)} links")
    except IOError as e:
        print(f"Error writing to {README_FILE}: {e}", file=sys.stderr)


def main():
    """Основная функция выполнения скрипта."""
    entries = read_sources()
    clear_data_dir()

    # Скачивание файлов с использованием ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download, e) for e in entries]
        
        has_errors = False
        for future in as_completed(futures):
            entry, success, error = future.result()
            if success:
                print(f"✓ Downloaded {entry['url']} as {entry['filename']}")
            else:
                print(f"✗ Error downloading {entry['url']}: {error}", file=sys.stderr)
                has_errors = True
        
        if has_errors:
            print("\nSome downloads failed!", file=sys.stderr)
            sys.exit(1)

    update_readme(entries)
    print("\nAll operations completed successfully!")


if __name__ == '__main__':
    main()
