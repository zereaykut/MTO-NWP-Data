from src.base import BaseDownloader
import datetime as dt

class GFSDownloader(BaseDownloader):
    def get_forecast_steps(self, run_time: dt.datetime, domain: str) -> list:
        if "ens" in domain:
            return list(range(0, 241, 3))
        return list(range(0, 120, 1)) + list(range(120, 385, 3))

    def build_urls(self, domain: str, run_time: dt.datetime, step: int) -> list:
        # Ported logic from build_gfs_urls
        yyyymmdd = run_time.strftime("%Y%m%d")
        hh = run_time.strftime("%H")
        return [f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{yyyymmdd}/{hh}/atmos/gfs.t{hh}z.pgrb2.0p25.f{step:03d}"]