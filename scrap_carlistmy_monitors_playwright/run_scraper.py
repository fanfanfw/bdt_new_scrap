import argparse
from scrap_carlistmy_monitors_playwright.carlistmy_service import CarlistMyService
from dotenv import load_dotenv

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Scrape data dari carlist.my")
    args = parser.parse_args()

    scraper = CarlistMyService()
    try:
        scraper.scrape_all_brands()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
