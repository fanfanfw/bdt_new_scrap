import argparse
from scrap_mudahmy_monitors_playwright.mudahmy_service import MudahMyService
from dotenv import load_dotenv

load_dotenv()

def main():
    scraper = MudahMyService()
    try:
        # Langsung mengatur start_page ke 1, dan hanya memproses halaman 1 dan 2
        scraper.scrape_all_from_main(start_page=1, descending=False)
    finally:
        scraper.close()

if __name__ == "__main__":
    main()

