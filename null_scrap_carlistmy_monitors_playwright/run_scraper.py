import argparse
from null_scrap_carlistmy_monitors_playwright.carlist_null_service import CarlistMyNullService
from dotenv import load_dotenv

load_dotenv(override=True)

def main():
    parser = argparse.ArgumentParser(description="Scrape data ulang (null fields) dari carlist.my")
    parser.add_argument('--image-download', choices=['yes', 'no'], default='yes', help="Download images locally or not")

    args = parser.parse_args()

    download_images_locally = args.image_download == 'yes'

    scraper = CarlistMyNullService(download_images_locally=download_images_locally)
    try:
        scraper.scrape_null_entries()
    finally:
        scraper.quit_browser()
        scraper.conn.close()

if __name__ == "__main__":
    main()
