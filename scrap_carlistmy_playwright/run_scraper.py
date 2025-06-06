import argparse
from scrap_carlistmy_playwright.carlistmy_service import CarlistMyService
from dotenv import load_dotenv

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Scrape data dari carlist.my")
    parser.add_argument("--page", type=int, default=1, help="Halaman awal (default=1)")
    parser.add_argument("--max-pages", type=int, default=None, help="Batas maksimal halaman (opsional)")
    args = parser.parse_args()

    scraper = CarlistMyService()
    try:
        scraper.scrape_all_brands(start_page=args.page, max_pages=args.max_pages)
    finally:
        scraper.close()

if __name__ == "__main__":
    main()