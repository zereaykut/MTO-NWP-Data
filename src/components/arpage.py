from src.base import BaseDownloader
import datetime as dt

class ArpegeDownloader(BaseDownloader):
    def get_package_times(self):
        # Arpege uses package blocks like 00H12H, 13H24H
        return ["00H12H", "13H24H", "25H36H", "37H48H"]

    def build_urls(self, run_time: dt.datetime, package: str, time_group: str, domain: str = "arpege_world"):
        date_str = run_time.strftime("%Y%m%d")
        hh = run_time.strftime("%H")
        # MeteoFrance Open Data via gouv.fr
        url = f"https://object.data.gouv.fr/arpege-nwpy-open-data/{domain}/{date_str}/{hh}/{package}/{time_group}.grib2"
        return [url]