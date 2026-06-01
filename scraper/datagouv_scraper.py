import os
import requests
import duckdb
from pathlib import Path
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from enum import Enum

MAX_WORKERS = 8
TIMEOUT = 60

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

class ResourceStatus(Enum):
    OK = 1
    SKIP = 2
    ERROR = 3


logger = logging.getLogger()
session = requests.Session()
session.headers.update(HEADERS)


def convert_csv_to_parquet(csv_path, parquet_path):
    with duckdb.connect() as conn:
        conn.execute(f"""
            COPY (
                SELECT *
                FROM read_csv_auto(
                    '{csv_path}',
                    delim=';',
                    header=true,
                    ignore_errors=true
                )
            )
            TO '{parquet_path}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD
            )
        """)


def convert_to_parquet(file_path):
    csv_path = file_path
    parquet_path = csv_path.replace('.csv.gz', '.parquet')

    try:
        convert_csv_to_parquet(
            csv_path,
            parquet_path
        )
    except Exception as e:
        raise e
    finally:
        # cleanup
        if os.path.exists(csv_path):
            os.remove(csv_path)


def download_stream(url, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    tmp_path = out_path + ".part"

    with session.get(url, stream=True, timeout=TIMEOUT) as r:
        r.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    os.rename(tmp_path, out_path)


def download_resource(resource, metadata, download_dir):
    base_download_path = download_dir
    need_parquet_convertion = False
    target_format = resource['format']
    url = resource['url']
    title, _ = os.path.splitext(resource['title'])
    checksum = resource['extras'].get('analysis:checksum')
    logger.info(f"Downloading resource {title}")

    if resource['type'] == 'main':
        base_download_path = os.path.join(base_download_path, 'data')
        target_format = 'parquet'
        need_parquet_convertion = True
    elif resource['type'] == 'documentation':
        base_download_path = os.path.join(base_download_path, 'doc')

    download_path = os.path.join(
        base_download_path,
        resource['id'] + ''.join(Path(url).suffixes)
    )
    final_path = os.path.join(base_download_path, title + f'.{target_format}')

    if os.path.exists(final_path):
        if metadata.get(title) != checksum or not checksum:
            # Update file
            logger.info(f'Updating {title}')
            os.remove(final_path)
        else:
            # Skip file
            logger.info(f"[SKIP] {title}")
            return ResourceStatus.SKIP
    
    try:
        download_stream(url, download_path)

        if need_parquet_convertion:
            logger.info(f'Converting {title} to parquet')
            convert_to_parquet(download_path)
            download_path = download_path.replace('.csv.gz', '.parquet')
        
        # Rename to resource title
        os.rename(download_path, final_path)
    except Exception as e:
        logger.error(f"[ERROR] {title}: {e}")
        return ResourceStatus.ERROR
    
    metadata[title] = checksum
    logger.info(f"[OK] {title}")
    return ResourceStatus.OK


def load_metadata(metadata_file):
    if os.path.exists(metadata_file):
        with open(metadata_file) as f:
            return json.load(f)
    else:
        return {}

def save_metadata(metadata, metadata_file):
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f)


def main(dataset_id, download_dir):
    os.makedirs(download_dir, exist_ok=True)
    dataset = session.get(f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/", timeout=TIMEOUT).json()
    # with open(f'dataset/{dataset_id}.json') as f:
    #     dataset = json.load(f)
    resources = dataset["resources"]
    print(f"Downloading {dataset['page']}")
    logger.info(f"Downloading {dataset['page']}")
    logger.info(f"Found {len(resources)} resources")

    metadata_file = os.path.join(download_dir, 'dataset.meta')
    metadata = load_metadata(metadata_file)

    try:
        progress_bar = tqdm(total=len(resources))
        # Download in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(download_resource, r, metadata, download_dir) for r in resources]
            for _ in as_completed(futures):
                progress_bar.update(1)
    except Exception:
        for future in futures:
            future.cancel()
        raise
    finally:
        save_metadata(metadata, metadata_file) # always save metadata


if __name__ == '__main__':
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(
        filename=f'logs/datagouv_scraper.log',
        filemode='w',
        encoding='utf-8',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    datasets = [
        {
            'id': '6569b51ae64326786e4e8e1a',
            'download_path': 'dataset/meteo_france_quot',
        },
        {
            'id': '6569b4473bedf2e7abad3b72',
            'download_path': 'dataset/meteo_france_hor',
        },
    ]

    for dataset in datasets:
        main(dataset['id'], dataset['download_path'])
