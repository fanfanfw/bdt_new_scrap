import argparse
from null_scrap_mudahmy_monitors_playwright.mudahmy_null_service import MudahMyNullService
from dotenv import load_dotenv

load_dotenv(override=True)

def main():
    parser = argparse.ArgumentParser(description="Scrape data dari mudah.my")
    parser.add_argument('--image-download', choices=['yes', 'no'], default='yes', help="Download images locally atau tidak")
    parser.add_argument('--id-min', type=int, default=None, help="Filter: mulai dari id ini")
    parser.add_argument('--id-max', type=int, default=None, help="Filter: sampai id ini")
    parser.add_argument('--urgent', action='store_true', help="Juga scrape jika field condition='URGENT' (meskipun tidak null)")
    args = parser.parse_args()

    download_images_locally = args.image_download == 'yes'

    scraper = MudahMyNullService(download_images_locally=download_images_locally)
    try:
        scraper.scrape_null_entries(
            id_min=args.id_min,
            id_max=args.id_max,
            include_urgent=args.urgent
        )
    finally:
        scraper.close()


if __name__ == "__main__":
    main()