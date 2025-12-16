import os
import json
import re
import requests
import argparse
import random
from pathlib import Path
from database import get_connection
from tqdm import tqdm
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv(override=True)

BASE_FOLDER = "images_mudah"
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "image_download_mudah.log")
DEFAULT_TABLE = "cars_scrap_mudahmy"
ARCHIVE_TABLE = "cars_scrap_mudahmy_archive"

raw_proxies = os.getenv("CUSTOM_PROXIES_MUDAH", "")
proxies_list = []

if raw_proxies:
    proxy_entries = raw_proxies.split(",")
    for entry in proxy_entries:
        ip, port, user, pwd = entry.strip().split(":")
        proxy_url = f"http://{user}:{pwd}@{ip}:{port}"
        proxies_list.append(proxy_url)

Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

def log_text(message):
    with open(LOG_FILE, "a") as f:
        f.write(message + "\n")

def get_status_for_id(id_):
    if not os.path.exists(LOG_FILE):
        return None 
    with open(LOG_FILE) as f:
        for line in f:
            if f"[ID {id_}]" in line:
                if "FAILED" in line:
                    return "FAILED"
                elif "PARTIAL" in line:
                    return "PARTIAL"
                elif "SUCCESS" in line:
                    return "SUCCESS"
    return None  # ID belum diproses

def update_status_in_log(id_, status, downloaded, failed):
    """
    Fungsi untuk memperbarui status ID pada log setelah proses download.
    """
    with open(LOG_FILE, "r") as f:
        lines = f.readlines()

    with open(LOG_FILE, "w") as f:
        for line in lines:
            if f"[ID {id_}]" in line:
                # Hapus baris lama jika ID sudah ada
                continue
            f.write(line)
        
        # Tambahkan baris baru dengan status yang diperbarui
        f.write(f"[ID {id_}] {status}: {downloaded} downloaded, {failed} failed\n")

def get_random_proxy():
    if not proxies_list:
        return None
    return random.choice(proxies_list)

proxy = get_random_proxy()
proxies = {"http": proxy, "https": proxy} if proxy else None

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

def is_valid_url(url):
    """
    Fungsi untuk memastikan URL yang diberikan adalah string yang valid dan tidak kosong
    """
    if not url or not isinstance(url, str):
        return False
    return url.startswith("http")

def download_image(url, save_path):
    try:
        if not is_valid_url(url):
            print(f"❌ URL tidak valid: {url}")
            return


        response = requests.get(url, timeout=20, proxies=proxies)
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
        SELECT id, brand, model, variant, year, images
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
        id_, brand, model, variant, year, images_str = row

        brand = brand or "UNKNOWN"
        model = model or "UNKNOWN"
        variant = variant or "UNKNOWN"
        year_segment = "UNKNOWN_YEAR"
        if year is not None:
            match = re.search(r"\d{4}", str(year))
            if match:
                year_segment = match.group(0)
            else:
                cleaned_year = str(year).strip()
                year_segment = cleaned_year if cleaned_year else "UNKNOWN_YEAR"

        folder_path = os.path.join(BASE_FOLDER, brand, model, variant, year_segment, str(id_))
        create_folder(folder_path)

        sukses, gagal = 0, 0

        try:
            images_list = json.loads(images_str)
            expected_count = len(images_list)

            status = get_status_for_id(id_)

            # Skip hanya jika sudah lengkap di filesystem dan tercatat sukses
            if status == "SUCCESS" and has_complete_download(Path(folder_path), expected_count):
                print(f"✅ Melewati ID {id_} (folder sudah lengkap dan status SUCCESS)")
                continue

            print(f"🔁 Memulai download untuk ID {id_}")

            for img_url in images_list:
                filename = os.path.basename(urlparse(img_url).path) or "image.jpg"
                save_path = os.path.join(folder_path, filename)

                if os.path.exists(save_path):
                    sukses += 1
                    continue

                if download_image(img_url, save_path):
                    sukses += 1
                else:
                    gagal += 1

            # Jika ada download yang gagal, ubah status menjadi FAILED
            if gagal == 0:
                log_text(f"[ID {id_}] ✅ SUCCESS: {sukses} downloaded, {gagal} failed")
                update_status_in_log(id_, "SUCCESS", sukses, gagal)  # Update status ke SUCCESS
            elif sukses > 0:
                log_text(f"[ID {id_}] ⚠️ PARTIAL: {sukses} downloaded, {gagal} failed")
                update_status_in_log(id_, "PARTIAL", sukses, gagal)
            else:
                log_text(f"[ID {id_}] ❌ FAILED: {sukses} downloaded, {gagal} failed")
                update_status_in_log(id_, "FAILED", sukses, gagal)  # Update status ke FAILED

        except Exception as e:
            print(f"❌ Error parsing images id={id_}: {e}")
            log_text(f"[ID {id_}] ❌ ERROR: Failed to parse images")
            update_status_in_log(id_, "FAILED", 0, 0)

    cursor.close()
    conn.close()
    print("✅ Proses download selesai")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloader gambar dari database.")
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
