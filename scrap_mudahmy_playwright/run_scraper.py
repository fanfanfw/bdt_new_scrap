import argparse
from scrap_mudahmy_playwright.mudahmy_service import MudahMyService
from dotenv import load_dotenv

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Scrape data dari mudah.my (halaman utama)")
    parser.add_argument("--page", type=int, default=1, help="Halaman awal (default=1)")
    parser.add_argument("--descending", action="store_true", help="Scraping dari halaman besar ke kecil")
    args = parser.parse_args()

    scraper = MudahMyService()
    try:
        scraper.scrape_all_from_main(start_page=args.page, descending=args.descending)
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
