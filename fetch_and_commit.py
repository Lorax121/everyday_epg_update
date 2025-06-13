import os
import sys
import json
import re
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


def slugify(text: str) -> str:
    rus_to_eng = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo', 'ж': 'zh',
        'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o',
        'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts',
        'ч': 'ch', 'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
    }
    text = text.lower()
    slug = ''.join(rus_to_eng.get(char, char) for char in text)
    slug = re.sub(r'[^\w\s-]', '', slug).strip()
    slug = re.sub(r'[-\s]+', '-', slug)
    return slug or "unnamed"


def read_sources_and_notes():
    try:
        with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
            sources = config.get('sources', [])
            notes = config.get('notes', '')
            if not sources:
                sys.exit(1)
            descriptions = [s['desc'] for s in sources]
            if len(descriptions) != len(set(descriptions)):
                 sys.exit(1)
            return sources, notes
    except FileNotFoundError:
        sys.exit(1)
    except json.JSONDecodeError:
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
    return ''.join(suffixes) if suffixes else '.dat'


def shorten_url(url):
    try:
        shortener = gdshortener.ISGDShortener()
        return shortener.shorten(url)
    except Exception as e:
        return "не удалось сократить"


def download_one(entry, url_templates):
    url = entry['url']
    desc = entry['desc']
    temp_path = DATA_DIR / ("tmp_" + os.urandom(4).hex())
    
    result = {'desc': desc, 'url': url, 'error': None}

    try:
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

        ext = detect_extension(temp_path, url)
        base_name = slugify(desc)
        filename = f"{base_name}{ext}"
        target_path = DATA_DIR / filename
        temp_path.rename(target_path)


        raw_url = url_templates['raw'].format(filename=filename)
        jsdelivr_url = url_templates['jsdelivr'].format(filename=filename)
        
        with ThreadPoolExecutor(max_workers=2) as shortener_executor:
            future_raw = shortener_executor.submit(shorten_url, raw_url)
            future_jsdelivr = shortener_executor.submit(shorten_url, jsdelivr_url)
            
            short_raw_url = future_raw.result()
            short_jsdelivr_url = future_jsdelivr.result()

        result.update({
            'size_mb': size_mb,
            'raw_url': raw_url,
            'jsdelivr_url': jsdelivr_url,
            'short_raw_url': short_raw_url,
            'short_jsdelivr_url': short_jsdelivr_url,
        })
        return result

    except requests.exceptions.RequestException as e:
        result['error'] = f"Ошибка загрузки: {e}"
        if temp_path.exists():
            temp_path.unlink()
        return result
    except Exception as e:
        result['error'] = f"Неизвестная ошибка: {e}"
        if temp_path.exists():
            temp_path.unlink()
        return result


def update_readme(results, notes):
    utc_now = datetime.now(timezone.utc)
    timestamp = utc_now.strftime('%Y-%m-%d %H:%M %Z')
    
    lines = [f"# Обновлено: {timestamp}", ""]
    for idx, r in enumerate(results, 1):
        lines.append(f"### {idx}. {r['desc']}")
        lines.append("")
        if r.get('error'):
            lines.append(f"**Статус:** 🔴 Ошибка")
            lines.append(f"**Источник:** `{r['url']}`")
            lines.append(f"**Причина:** {r.get('error')}")
        else:
            lines.append(f"**Статус:** 🟢 Успешно")
            lines.append(f"**Размер:** {r['size_mb']} MB")
            lines.append("")
            lines.append(f"- **Прямая ссылка (GitHub Raw):**")
            lines.append(f"  - `{r['raw_url']}`")
            lines.append(f"  - Короткая: `{r['short_raw_url']}`")
            lines.append(f"- **CDN ссылка (jsDelivr):**")
            lines.append(f"  - `{r['jsdelivr_url']}`")
            lines.append(f"  - Короткая: `{r['short_jsdelivr_url']}`")
        lines.append("")
        lines.append("---")
        lines.append("")

    if notes:
        lines.append("## Примечания")
        lines.append("")
        lines.append(notes)
        lines.append("")

    with open(README_FILE, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))
    print(f"README.md обновлён ({len(results)} записей)")


def main():
    repo = os.getenv('GITHUB_REPOSITORY')
    if not repo or '/' not in repo:
        sys.exit(1)
    
    owner, repo_name = repo.split('/')
    
    url_templates = {
        'raw': RAW_BASE_URL.format(owner=owner, repo=repo_name, filename="{filename}"),
        'jsdelivr': JSDELIVR_BASE_URL.format(owner=owner, repo=repo_name, filename="{filename}")
    }

    sources, notes = read_sources_and_notes()
    clear_data_dir()

    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_entry = {executor.submit(download_one, entry, url_templates): entry for entry in sources}
        for future in as_completed(future_to_entry):
            all_results.append(future.result())

    url_to_result = {res['url']: res for res in all_results}
    ordered_results = [url_to_result[s['url']] for s in sources]

    update_readme(ordered_results, notes)


if __name__ == '__main__':
    main()
