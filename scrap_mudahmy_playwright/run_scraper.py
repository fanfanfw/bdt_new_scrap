import argparse
from scrap_mudahmy_playwright.mudahmy_service import MudahMyService
from dotenv import load_dotenv

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Scrape data dari mudah.my (halaman utama)")
    args = parser.parse_args()

    scraper = MudahMyService()
    try:
        scraper.scrape_all_from_main()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
