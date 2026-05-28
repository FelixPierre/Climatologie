import os
import requests
import duckdb
from pathlib import Path
import json


# DATASET_ID = "6569b4473bedf2e7abad3b72" # Données climatologiques de base - horaire
DATASET_ID = "6569b51ae64326786e4e8e1a" # Données climatologiques de base - quotidiennes
DOWNLOAD_DIR = "dataset/datagouv_meteo_france_quot"

TIMEOUT = 60

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

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


def download_resource(resource, metadata):
    base_download_path = DOWNLOAD_DIR
    need_parquet_convertion = False
    target_format = resource['format']
    url = resource['url']
    title, _ = os.path.splitext(resource['title'])
    checksum = resource['extras']['analysis:checksum']

    if resource['type'] == 'main':
        base_download_path = os.path.join(base_download_path, 'data')
        target_format = 'parquet'
        # Take parquet url if possible
        parquet_url = resource.get('extras', {}).get('analysis:parsing:parquet_url')
        if parquet_url:
            url = parquet_url
        else:
            need_parquet_convertion = True
    elif resource['type'] == 'documentation':
        base_download_path = os.path.join(base_download_path, 'doc')

    download_path = os.path.join(
        base_download_path,
        resource['id'] + ''.join(Path(url).suffixes)
    )
    final_path = os.path.join(base_download_path, title + f'.{target_format}')

    # if os.path.exists(final_path):
    #     return(
    #         "[SKIP] "
    #         f"{os.path.basename(final_path)}"

    if os.path.exists(final_path):
        if metadata.get(title) != checksum:
            # Update file
            print(f'updating')
            os.remove(final_path)
        else:
            # Skip
            return(
                "[SKIP] "
                f"{os.path.basename(final_path)}"
            )
    
    try:
        download_stream(url, download_path)

        if need_parquet_convertion:
            print(f'converting to parquet : {download_path}')
            convert_to_parquet(download_path)
            download_path = download_path.replace('.csv.gz', '.parquet')
        
        # Rename to resource title
        os.rename(download_path, final_path)
    except Exception as e:
        return (
            "[ERR] "
            f"{os.path.basename(final_path)}"
            f"\n{e}"
        )
    
    metadata[title] = checksum
    return (
        "[OK] "
        f"{os.path.basename(final_path)}"
    )


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
    resources = dataset["resources"]
    print(f"Found {len(resources)} resources")

    metadata_file = os.path.join(download_dir, 'dataset.meta')
    metadata = load_metadata(metadata_file)

    for r in resources:
        print(f'Downloading {r['title']}')
        ret = download_resource(r, metadata)
        print(ret)

    save_metadata(metadata, metadata_file)


if __name__ == '__main__':
    main(DATASET_ID, DOWNLOAD_DIR)
