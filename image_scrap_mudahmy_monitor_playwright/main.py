import os
import json
import requests
import argparse
from pathlib import Path
from database import get_connection
from tqdm import tqdm

BASE_FOLDER = "images_mudah"

def create_folder(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def download_image(url, save_path):
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        with open(save_path, "wb") as f:
            f.write(response.content)
    except Exception as e:
        print(f"❌ Gagal download {url} -> {e}")

def main(start_id=None, end_id=None):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT id, brand, model, variant, images
        FROM cars_scrap
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

        brand = brand or "UNKNOWN"
        model = model or "UNKNOWN"
        variant = variant or "UNKNOWN"

        folder_path = os.path.join(BASE_FOLDER, brand, model, variant, str(id_))
        create_folder(folder_path)

        try:
            images_list = json.loads(images_str)

            for img_url in images_list:
                filename = img_url.split("/")[-1]
                save_path = os.path.join(folder_path, filename)

                if os.path.exists(save_path):
                    print(f"✔ File sudah ada: {save_path}")
                    continue

                download_image(img_url, save_path)

        except Exception as e:
            print(f"❌ Error parsing images id={id_}: {e}")

    cursor.close()
    conn.close()
    print("✅ Proses download selesai")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Downloader gambar dari database.")
    parser.add_argument("--start-id", type=int, help="Mulai dari ID (inclusive)")
    parser.add_argument("--end-id", type=int, help="Sampai ID (inclusive)")
    args = parser.parse_args()

    main(start_id=args.start_id, end_id=args.end_id)