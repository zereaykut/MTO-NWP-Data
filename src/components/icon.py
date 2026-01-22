from src.base import BaseDownloader
import datetime as dt

class IconDownloader(BaseDownloader):
    def get_forecast_steps(self, domain: str = "icon-eu"):
        if domain == "icon-eu":
            return list(range(0, 79, 1)) + list(range(81, 121, 3))
        return list(range(0, 181, 3))

    def build_urls(self, run_time: dt.datetime, step: int, variable: str, domain: str = "icon-eu"):
        date_str = run_time.strftime("%Y%m%d%H")
        # DWD Open Data uses bz2 compression
        base = "https://opendata.dwd.de/weather/nwp"
        url = f"{base}/{domain}/grib/{run_time.strftime('%H')}/{variable}/" \
              f"icon-eu_europe_regular-lat-lon_single-level_{date_str}_{step:03d}_{variable.upper()}.grib2.bz2"
        return [url]