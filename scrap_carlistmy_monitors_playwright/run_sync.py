from scrap_carlistmy_monitors_playwright.carlistmy_service import CarlistMyService
from dotenv import load_dotenv

load_dotenv()

def main():
    scraper = CarlistMyService()
    try:
        scraper.sync_to_cars()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()