import argparse
from scrap_carlistmy_monitors_playwright.carlistmy_service import CarlistMyService
from dotenv import load_dotenv

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Scrape data dari carlist.my")
    parser.add_argument('--image-download', choices=['yes', 'no'], default='yes', help="Download images locally or not")

    args = parser.parse_args()

    download_images_locally = args.image_download == 'yes'

    scraper = CarlistMyService(download_images_locally=download_images_locally)
    try:
        scraper.scrape_all_brands()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
