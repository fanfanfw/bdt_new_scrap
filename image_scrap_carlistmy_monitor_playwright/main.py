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

def main(start_id=None, end_id=None):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT id, brand, model, variant, images
        FROM cars_scrap_carlistmy
        WHERE images IS NOT NULL AND images != ''
    """
    params = []

    if start_id is not None:
        query += " AND id >= %s"
        params.append(start_id)
    if end_id is not None:
        query += " AND id <= %s"
        params.append(end_id)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    print(f"Total data ditemukan: {len(rows)}")

    for row in tqdm(rows):
        id_, brand, model, variant, images_str = row

        if is_id_logged(id_):
            print(f"🔁 Melewati ID {id_} (sudah di-log)")
            continue

        brand = brand or "UNKNOWN"
        model = model or "UNKNOWN"
        variant = variant or "UNKNOWN"

        folder_path = os.path.join(BASE_FOLDER, brand, model, variant, str(id_))
        create_folder(folder_path)

        sukses, gagal = 0, 0

        try:
            images_list = json.loads(images_str)

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
    args = parser.parse_args()

    main(start_id=args.start_id, end_id=args.end_id)