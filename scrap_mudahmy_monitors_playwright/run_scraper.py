import argparse
from scrap_mudahmy_monitors_playwright.mudahmy_service import MudahMyService
from dotenv import load_dotenv

load_dotenv(override=True)

def main():
    parser = argparse.ArgumentParser(description="Scrape data dari mudah.my")
    parser.add_argument('--image-download', choices=['yes', 'no'], default='yes', help="Download images locally atau tidak")
    args = parser.parse_args()

    download_images_locally = args.image_download == 'yes'

    scraper = MudahMyService(download_images_locally=download_images_locally)
    try:
        scraper.scrape_all_from_main()
    finally:
        scraper.close()

if __name__ == "__main__":
    main()