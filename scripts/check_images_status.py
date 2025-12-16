import argparse
import json
import os
import re
import sys
from pathlib import Path

# Ensure repository root is on sys.path so module imports work when run from anywhere
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scrap_carlistmy_monitors_playwright.database import get_connection as get_carlist_conn
from scrap_mudahmy_monitors_playwright.database import get_connection as get_mudah_conn


def normalize_segment(value: str) -> str:
    """
    Normalize brand/model/variant for folder naming.
    Fallback to UNKNOWN and replace "/" to keep path safe.
    """
    if not value:
        return "UNKNOWN"
    return str(value).strip().upper().replace("/", "_")

def normalize_year(value) -> str:
    """
    Normalize year for folder naming. Falls back to UNKNOWN_YEAR when missing.
    """
    if value is None:
        return "UNKNOWN_YEAR"
    match = re.search(r"\d{4}", str(value))
    if match:
        return match.group(0)
    cleaned = str(value).strip()
    return cleaned if cleaned else "UNKNOWN_YEAR"

def build_folder(base_folder: Path, brand: str, model: str, variant: str, year, car_id: int) -> Path:
    return (
        base_folder
        / normalize_segment(brand)
        / normalize_segment(model)
        / normalize_segment(variant)
        / normalize_year(year)
        / str(car_id)
    )


def index_existing_folders(base_folder: Path):
    """
    Build a mapping from numeric folder name (car_id) to the actual folder path.
    This avoids repeated rglob for every row.
    """
    mapping = {}
    if not base_folder.exists():
        return mapping
    for path in base_folder.rglob("*"):
        if path.is_dir() and path.name.isdigit():
            mapping[int(path.name)] = path
    return mapping


def check_site(name: str, conn, table: str, base_folder: Path, limit: int = None):
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT id, brand, model, variant, year, images
        FROM {table}
        WHERE images IS NOT NULL AND images != ''
        """
    )
    rows = cursor.fetchall()

    total_rows = len(rows)
    complete = []
    partial = []
    missing = []
    json_error = []

    existing_folders = index_existing_folders(base_folder)

    for idx, (car_id, brand, model, variant, year, images_str) in enumerate(rows, start=1):
        if limit and idx > limit:
            break

        try:
            images_list = json.loads(images_str)
            expected = len(images_list)
        except Exception:
            json_error.append(car_id)
            continue

        folder = build_folder(base_folder, brand, model, variant, year, car_id)
        legacy_folder = (
            base_folder
            / normalize_segment(brand)
            / normalize_segment(model)
            / normalize_segment(variant)
            / str(car_id)
        )
        if not folder.exists() and legacy_folder.exists():
            folder = legacy_folder
        if not folder.exists():
            # Fallback: use any folder that matches the car_id regardless of brand/model/variant
            folder = existing_folders.get(car_id)

        if folder is None or not Path(folder).exists():
            missing.append(car_id)
            continue

        # Count files only (ignore subdirs)
        actual = len([p for p in Path(folder).iterdir() if p.is_file()])

        if expected == 0:
            # Nothing to download, treat as complete
            complete.append(car_id)
        elif actual >= expected:
            complete.append(car_id)
        elif actual == 0:
            missing.append(car_id)
        else:
            partial.append(car_id)

    print(f"\n=== {name.upper()} ===")
    print(f"Rows checked   : {len(complete) + len(partial) + len(missing) + len(json_error)} / {total_rows}")
    print(f"Complete       : {len(complete)}")
    print(f"Partial        : {len(partial)}")
    print(f"Missing folder : {len(missing)}")
    print(f"JSON errors    : {len(json_error)}")

    if partial:
        print(f"Partial IDs (first 20): {partial[:20]}")
    if missing:
        print(f"Missing IDs (first 20): {missing[:20]}")
    if json_error:
        print(f"JSON error IDs (first 20): {json_error[:20]}")

    cursor.close()


def main():
    parser = argparse.ArgumentParser(description="Check downloaded images against database records.")
    parser.add_argument(
        "--site",
        choices=["carlist", "mudah", "both"],
        default="both",
        help="Site to check",
    )
    parser.add_argument(
        "--carlist-table",
        default=os.getenv("DB_TABLE_SCRAP_CARLIST", "cars_scrap_new"),
        help="Table name for Carlist (defaults to env DB_TABLE_SCRAP_CARLIST)",
    )
    parser.add_argument(
        "--mudah-table",
        default=os.getenv("DB_TABLE_SCRAP_MUDAH", "cars_scrap_mudahmy"),
        help="Table name for Mudah (defaults to env DB_TABLE_SCRAP_MUDAH)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional limit of rows to scan (per site)",
    )
    parser.add_argument(
        "--carlist-folder",
        default="images_carlist",
        help="Base folder for Carlist images",
    )
    parser.add_argument(
        "--mudah-folder",
        default="images_mudah",
        help="Base folder for Mudah images",
    )
    args = parser.parse_args()

    if args.site in ("carlist", "both"):
        conn = get_carlist_conn()
        try:
            check_site(
                "carlist",
                conn,
                args.carlist_table,
                Path(args.carlist_folder),
                limit=args.limit,
            )
        finally:
            conn.close()

    if args.site in ("mudah", "both"):
        conn = get_mudah_conn()
        try:
            check_site(
                "mudah",
                conn,
                args.mudah_table,
                Path(args.mudah_folder),
                limit=args.limit,
            )
        finally:
            conn.close()


if __name__ == "__main__":
    main()
