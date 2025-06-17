from null_scrap_mudahmy_monitors_playwright.mudahmy_null_service import MudahMyNullService
from dotenv import load_dotenv

load_dotenv()

def main():
    scraper = MudahMyNullService()
    try:
        scraper.sync_to_cars()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
