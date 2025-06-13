
import os
import sys
import json
import requests
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import gdshortener

SOURCES_FILE = 'sources.json'
DATA_DIR = Path('data')
README_FILE = 'README.md'
MAX_WORKERS = 8
CHUNK_SIZE = 16 * 1024
MAX_FILE_SIZE_MB = 95

RAW_BASE_URL = "https://github.com/{owner}/{repo}/raw/main/data/{filename}"
JSDELIVR_BASE_URL = "https://cdn.jsdelivr.net/gh/{owner}/{repo}@main/data/{filename}"


def read_sources_and_notes():
    try:
        with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            sources = config.get('sources', [])
            notes = config.get('notes', '')
            if not sources:
                print("Ошибка: в sources.json не найдено ни одного источника в ключе 'sources'.", file=sys.stderr)
                sys.exit(1)
            return sources, notes
    except FileNotFoundError:
        print(f"Ошибка: Файл {SOURCES_FILE} не найден.", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Ошибка: Некорректный формат JSON в файле {SOURCES_FILE}.", file=sys.stderr)
        sys.exit(1)


def clear_data_dir():
    if DATA_DIR.exists():
        for f in DATA_DIR.iterdir():
            if f.is_file():
                f.unlink()
    else:
        DATA_DIR.mkdir(parents=True)


def detect_extension(file_path, url):
    with open(file_path, 'rb') as f:
        sig = f.read(5)
    if sig[:2] == b"\x1f\x8b":
        return '.xml.gz'
    if sig.startswith(b'<?xml'):
        return '.xml'
    suffixes = Path(urlparse(url).path).suffixes
    return ''.join(suffixes) 


def download_one(entry):
    url = entry['url']
    desc = entry['desc']
    temp_path = DATA_DIR / ("tmp_" + os.urandom(4).hex())
    
    result = {'desc': desc, 'url': url, 'error': None}

    try:
        print(f"Начинаю загрузку: {desc} ({url})")
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(temp_path, 'wb') as f:
                for chunk in r.iter_content(CHUNK_SIZE):
                    f.write(chunk)
        
        size_bytes = temp_path.stat().st_size
        size_mb = round(size_bytes / (1024 * 1024), 2)
        
        if size_bytes > MAX_FILE_SIZE_MB * 1024 * 1024:
            error_msg = f"Файл слишком большой ({size_mb} MB > {MAX_FILE_SIZE_MB} MB). Пропускаем."
            print(error_msg)
            result['error'] = error_msg
            temp_path.unlink()
            return result

        true_extension = detect_extension(temp_path, url)

        filename_from_url = Path(urlparse(url).path).name or "download"

        base_name = filename_from_url.split('.')[0]
        proposed_filename = f"{base_name}{true_extension}"
        
        result.update({
            'size_mb': size_mb,
            'temp_path': temp_path,
            'proposed_filename': proposed_filename
        })
        return result

    except requests.exceptions.RequestException as e:
        result['error'] = f"Ошибка загрузки: {e}"
    except Exception as e:
        result['error'] = f"Неизвестная ошибка: {e}"
        
    print(f"Ошибка для {desc}: {result['error']}")
    if temp_path.exists():
        temp_path.unlink()
    return result


def shorten_url_safely(url):
    try:
        shortener = gdshortener.ISGDShortener()
        short_tuple = shortener.shorten(url)
        return short_tuple[0] if short_tuple and short_tuple[0] else "не удалось сократить"
    except Exception as e:
        print(f"Не удалось сократить URL {url}: {e}", file=sys.stderr)
        return "не удалось сократить"

def update_readme(results, notes):
    utc_now = datetime.now(timezone.utc)
    timestamp = utc_now.strftime('%Y-%m-%d %H:%M %Z')
    
    lines = []

    if notes:
        lines.append(notes)
        lines.append("\n---")
    
    lines.append(f"\n# Обновлено: {timestamp}\n")

    for idx, r in enumerate(results, 1):
        lines.append(f"### {idx}. {r['desc']}")
        lines.append("")
        if r.get('error'):
            lines.append(f"**Статус:** 🔴 Ошибка")
            lines.append(f"**Источник:** `{r['url']}`")
            lines.append(f"**Причина:** {r.get('error')}")
        else:
            lines.append(f"**Размер:** {r['size_mb']} MB")
            lines.append("")
            lines.append(f"- **Прямая ссылка (GitHub Raw):**")
            lines.append(f"  - `{r['raw_url']}`")
            lines.append(f"  - Короткая: `{r['short_raw_url']}`")
            lines.append(f"- **CDN ссылка (jsDelivr):**")
            lines.append(f"  - `{r['jsdelivr_url']}`")
            lines.append(f"  - Короткая: `{r['short_jsdelivr_url']}`")
        lines.append("\n---")

    with open(README_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))
    print(f"README.md обновлён ({len(results)} записей)")


def main():
    repo = os.getenv('GITHUB_REPOSITORY')
    if not repo or '/' not in repo:
        print("Ошибка: не удалось определить GITHUB_REPOSITORY.", file=sys.stderr)
        sys.exit(1)
    
    owner, repo_name = repo.split('/')
    
    sources, notes = read_sources_and_notes()
    clear_data_dir()

    temp_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_entry = {executor.submit(download_one, entry): entry for entry in sources}
        for future in as_completed(future_to_entry):
            temp_results.append(future.result())

    url_to_result = {res['url']: res for res in temp_results}
    ordered_results = [url_to_result[s['url']] for s in sources]

    final_results = []
    used_names = set()
    for res in ordered_results:
        if res.get('error'):
            final_results.append(res)
            continue

        original_name = res['proposed_filename']
        final_name = original_name
        counter = 1
        while final_name in used_names:
            p = Path(original_name)
            final_name = f"{p.stem}-{counter}{p.suffix}"
            counter += 1
        
        used_names.add(final_name)
        
        target_path = DATA_DIR / final_name
        res['temp_path'].rename(target_path)
        
        raw_url = RAW_BASE_URL.format(owner=owner, repo=repo_name, filename=final_name)
        jsdelivr_url = JSDELIVR_BASE_URL.format(owner=owner, repo=repo_name, filename=final_name)
        
        res['raw_url'] = raw_url
        res['jsdelivr_url'] = jsdelivr_url
        res['short_raw_url'] = shorten_url_safely(raw_url)
        res['short_jsdelivr_url'] = shorten_url_safely(jsdelivr_url)
        
        final_results.append(res)

    update_readme(final_results, notes)
    print("Скрипт успешно завершил работу.")


if __name__ == '__main__':
    main()
