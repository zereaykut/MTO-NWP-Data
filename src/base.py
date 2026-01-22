import logging
import requests
from abc import ABC, abstractmethod
from pathlib import Path
import datetime as dt

class BaseDownloader(ABC):
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        logging.info(f"Initialized downloader for {self.__class__.__name__}")

    @abstractmethod
    def get_forecast_steps(self, run_time: dt.datetime, domain: str) -> list:
        """Calculate the forecast hours available for a given run."""
        pass

    @abstractmethod
    def build_urls(self, domain: str, run_time: dt.datetime, step: int) -> list:
        """Construct the download URLs for a specific forecast step."""
        pass

    def download(self, url: str, filename: str):
        target_path = self.output_dir / filename
        if target_path.exists():
            return
        
        try:
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            with open(target_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Success: {filename}")
        except Exception as e:
            logging.error(f"Error downloading {url}: {e}")