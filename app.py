import logging
import datetime as dt
from src.components.gfs import GFSDownloader
from src.components.ecmwf import ECMWFDownloader
from src.components.icon import IconDownloader
from src.components.arpage import ArpegeDownloader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline():
    # Use UTC for meteorological runs
    now = dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    
    models = {
        "gfs": GFSDownloader(output_dir="./data/gfs"),
        "ecmwf": ECMWFDownloader(output_dir="./data/ecmwf"),
        "icon": IconDownloader(output_dir="./data/icon"),
        "arpege": ArpegeDownloader(output_dir="./data/arpege")
    }

    for name, downloader in models.items():
        logging.info(f"Starting {name} download sequence...")
        steps = downloader.get_forecast_steps(now, domain="default")
        for step in steps[:5]:  # Limit for demo
            urls = downloader.build_urls("default", now, step)
            for url in urls:
                filename = url.split("/")[-1]
                downloader.download(url, filename)

if __name__ == "__main__":
    run_pipeline()