import os
import json
import requests
import argparse
from pathlib import Path
from urllib.parse import urlparse
from database import get_connection
from tqdm import tqdm

BASE_FOLDER = "images_carlist"
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "image_download_carlist.log")
DEFAULT_TABLE = "cars_scrap_carlistmy"
ARCHIVE_TABLE = "cars_scrap_carlistmy_archive"

# Pastikan folder log tersedia
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

def log_text(message):
    with open(LOG_FILE, "a") as f:
        f.write(message + "\n")

def is_id_logged(id_):
    if not os.path.exists(LOG_FILE):
        return False
    with open(LOG_FILE) as f:
        for line in f:
            if f"[ID {id_}]" in line:
                return True
    return False

def create_folder(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def has_complete_download(folder_path: Path, expected_count: int) -> bool:
    """
    Cek apakah folder sudah berisi minimal expected_count file.
    """
    if not folder_path.exists():
        return False
    files = [p for p in folder_path.iterdir() if p.is_file()]
    return len(files) >= expected_count if expected_count else True

def sanitize_filename(url):
    filename = os.path.basename(urlparse(url).path)
    return filename

def download_image(url, save_path):
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        with open(save_path, "wb") as f:
            f.write(response.content)
        return True
    except Exception as e:
        print(f"❌ Gagal download {url} -> {e}")
        return False

def main(
    start_id=None,
    end_id=None,
    brand_filter=None,
    model_filter=None,
    variant_filter=None,
    table_source=DEFAULT_TABLE,
):
    conn = get_connection()
    cursor = conn.cursor()

    table_name = DEFAULT_TABLE if table_source == DEFAULT_TABLE else ARCHIVE_TABLE

    query = f"""
        SELECT id, brand, model, variant, images
        FROM {table_name}
        WHERE images IS NOT NULL AND images != ''
    """
    params = []

    if start_id is not None:
        query += " AND id >= %s"
        params.append(start_id)
    if end_id is not None:
        query += " AND id <= %s"
        params.append(end_id)
    if brand_filter:
        query += " AND LOWER(brand) = LOWER(%s)"
        params.append(brand_filter)
    if model_filter:
        query += " AND LOWER(model) = LOWER(%s)"
        params.append(model_filter)
    if variant_filter:
        query += " AND LOWER(variant) = LOWER(%s)"
        params.append(variant_filter)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    print(f"Total data ditemukan: {len(rows)}")

    for row in tqdm(rows):
        id_, brand, model, variant, images_str = row

        brand = brand or "UNKNOWN"
        model = model or "UNKNOWN"
        variant = variant or "UNKNOWN"

        folder_path = os.path.join(BASE_FOLDER, brand, model, variant, str(id_))
        create_folder(folder_path)

        sukses, gagal = 0, 0

        try:
            images_list = json.loads(images_str)
            expected_count = len(images_list)

            # Skip hanya jika sudah lengkap di filesystem dan sudah tercatat di log
            if is_id_logged(id_) and has_complete_download(Path(folder_path), expected_count):
                print(f"✅ Melewati ID {id_} (sudah lengkap di folder dan tercatat)")
                continue

            for img_url in images_list:
                filename = sanitize_filename(img_url)
                save_path = os.path.join(folder_path, filename)

                if os.path.exists(save_path):
                    sukses += 1
                    continue

                if download_image(img_url, save_path):
                    sukses += 1
                else:
                    gagal += 1

            if gagal == 0:
                log_text(f"[ID {id_}] ✅ SUCCESS: {sukses} downloaded, {gagal} failed")
            elif sukses > 0:
                log_text(f"[ID {id_}] ⚠️ PARTIAL: {sukses} downloaded, {gagal} failed")
            else:
                log_text(f"[ID {id_}] ❌ FAILED: {sukses} downloaded, {gagal} failed")

        except Exception as e:
            print(f"❌ Error parsing images id={id_}: {e}")
            log_text(f"[ID {id_}] ❌ ERROR: Failed to parse images")

    cursor.close()
    conn.close()
    print("✅ Proses download selesai")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloader gambar Carlist dari database.")
    parser.add_argument("--start-id", type=int, help="Mulai dari ID (inclusive)")
    parser.add_argument("--end-id", type=int, help="Sampai ID (inclusive)")
    parser.add_argument("--brand", dest="brand_filter", help="Hanya unduh brand tertentu (exact match, case-insensitive)")
    parser.add_argument("--model", dest="model_filter", help="Hanya unduh model tertentu (exact match, case-insensitive)")
    parser.add_argument("--variant", dest="variant_filter", help="Hanya unduh variant tertentu (exact match, case-insensitive)")
    parser.add_argument(
        "--table",
        choices=[DEFAULT_TABLE, ARCHIVE_TABLE],
        default=DEFAULT_TABLE,
        help="Pilih tabel sumber data (normal atau archive)",
    )
    args = parser.parse_args()

    main(
        start_id=args.start_id,
        end_id=args.end_id,
        brand_filter=args.brand_filter,
        model_filter=args.model_filter,
        variant_filter=args.variant_filter,
        table_source=args.table,
    )
