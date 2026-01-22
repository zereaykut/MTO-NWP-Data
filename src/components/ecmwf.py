from src.base import BaseDownloader
import datetime as dt

class ECMWFDownloader(BaseDownloader):
    def get_forecast_steps(self, domain_type: str = "ifs025"):
        # Ported logic: 0-144h every 3h, then every 6h
        steps = list(range(0, 145, 3)) + list(range(150, 241, 6))
        return steps

    def build_urls(self, run_time: dt.datetime, step: int, domain: str = "ifs025"):
        date_str = run_time.strftime("%Y%m%d")
        hh = run_time.strftime("%H")
        # Example Open Data URL pattern
        base = "https://get.ecmwf.int/test-data/open-data/v1/realtime"
        url = f"{base}/{date_str}/{hh}z/{domain}/{step}h/oper/grib2.grib2"
        return [url]