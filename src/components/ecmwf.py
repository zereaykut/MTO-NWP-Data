#!/usr/bin/env python3
"""
ECMWF Open Data downloader component.
Updated for 2026 directory structure (aifs-single, aifs-ens, ifs/0p25).
"""

import argparse
import datetime as dt
import os
from pathlib import Path
from typing import List, Optional
import requests

# -------------------------------------------------------------------------
# Domain Logic
# -------------------------------------------------------------------------

class EcmwfDomain:
    # Map valid domain arguments to their configuration
    # (system_dir, resolution, default_stream, description)
    CONFIG = {
        "aifs025_single":   ("aifs-single", "0p25", "oper", "AIFS Deterministic"),
        "aifs025_ensemble": ("aifs-ens",    "0p25", "enfo", "AIFS Ensemble"),
        "ifs025":           ("ifs",         "0p25", "oper", "IFS Deterministic"),
        "ifs025_ensemble":  ("ifs",         "0p25", "enfo", "IFS Ensemble"),
        "wam025":           ("ifs",         "0p25", "wave", "IFS Wave Deterministic"),
        "wam025_ensemble":  ("ifs",         "0p25", "waef", "IFS Wave Ensemble"),
    }

    def __init__(self, name: str):
        # Allow simple aliases
        if name == "aifs025": name = "aifs025_single"
        if name == "ifs": name = "ifs025"
        
        if name not in self.CONFIG:
            raise ValueError(f"Unknown ECMWF domain: {name}. Valid options: {list(self.CONFIG.keys())}")
        
        self.name = name
        self.system, self.resolution, self.base_stream, self.desc = self.CONFIG[name]

    def get_download_forecast_steps(self, run_hour: int) -> List[int]:
        """
        Returns the list of available forecast hours (steps) based on the model and run time.
        """
        # AIFS is typically 6-hourly
        if "aifs" in self.name:
            return list(range(0, 361, 6))

        # IFS / WAM High Res logic
        # 00/12z: Hourly up to 144, then 3-hourly up to 144+? (OpenData varies, we use a safe subset)
        # Safe Subset for OpenData: 3-hourly is usually available for all steps.
        
        step_size = 3
        
        if run_hour in (0, 12):
            # IFS High Res goes out to 240h (10 days) in OpenData usually
            return list(range(0, 240 + step_size, step_size))
        elif run_hour in (6, 18):
            # SCDA (Short Cutoff) usually goes out to 90h
            return list(range(0, 90 + step_size, step_size))
        
        return []

    def get_urls(self, base_url: str, run_time: dt.datetime, step: int) -> List[str]:
        """
        Constructs the URL based on the provided directory links.
        Ref: https://data.ecmwf.int/forecasts/YYYYMMDD/HHz/{system}/{res}/{stream}/
        """
        # 1. Date Components
        yyyymmdd = run_time.strftime("%Y%m%d")
        hh = f"{run_time.hour:02d}"
        
        # 2. Determine Stream (Handle 06/18z SCDA for IFS)
        stream = self.base_stream
        
        # IFS Deterministic and Wave switch to 'scda'/'scwv' for 06/18z runs
        if self.name in ["ifs025", "wam025"] and run_time.hour in (6, 18):
            if stream == "oper": stream = "scda"
            if stream == "wave": stream = "scwv"
            
        # 3. Determine File Type (type-fc is standard, type-ep for ensemble prob sometimes)
        # For this script, we default to 'fc' (Forecast) as it's the main data file.
        ftype = "fc" 

        # 4. Construct URL
        # URL Base: https://data.ecmwf.int/forecasts/20260128/00z/
        url_base = f"{base_url}{yyyymmdd}/{hh}z"
        
        # Directory: ifs/0p25/oper/
        directory = f"{self.system}/{self.resolution}/{stream}"
        
        # Filename: 20260128000000-0h-oper-fc.grib2
        # Note: The time component in filename is usually YYYYMMDDHH0000
        file_date_str = f"{yyyymmdd}{hh}0000"
        filename = f"{file_date_str}-{step}h-{stream}-{ftype}.grib2"
        
        full_url = f"{url_base}/{directory}/{filename}"
        
        return [full_url]


# -------------------------------------------------------------------------
# Core Logic
# -------------------------------------------------------------------------

def download_ecmwf(
    domain: str,
    output_dir: Path,
    run: dt.datetime,
    max_forecast_hour: Optional[int] = None,
):
    """
    Main entry point for downloading ECMWF data.
    """
    try:
        dom = EcmwfDomain(domain)
    except ValueError as e:
        print(f"[ECMWF] Error: {e}")
        return

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Base URL for ECMWF Open Data
    base_url = "https://data.ecmwf.int/forecasts/"

    # Calculate steps
    try:
        forecast_hours = dom.get_download_forecast_steps(run.hour)
    except ValueError as e:
        print(f"[ECMWF] Error: {e}")
        return

    if max_forecast_hour is not None:
        forecast_hours = [h for h in forecast_hours if h <= max_forecast_hour]

    print(f"[ECMWF] Target: {dom.desc} ({dom.name})")
    print(f"[ECMWF] Run:    {run.isoformat()} (Steps: {len(forecast_hours)})")

    session = requests.Session()

    for hour in forecast_hours:
        urls = dom.get_urls(base_url, run, hour)
        for idx, url in enumerate(urls):
            # Local Filename: domain_date_run_hour.grib2
            fname = f"{dom.name}_{run.strftime('%Y%m%d')}_{run.hour:02d}z_{hour}h.grib2"
            out_path = output_dir / fname

            if out_path.exists():
                print(f"  [SKIP] {fname}")
                continue

            print(f"  [DOWN] {fname} ...")
            try:
                with session.get(url, stream=True, timeout=20) as r:
                    r.raise_for_status()
                    with open(out_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
            except Exception as e:
                # 404 is common for very late steps or specific streams not available
                print(f"  [ERR ] Failed to fetch: {url}")
                print(f"         Reason: {e}")

# -------------------------------------------------------------------------
# CLI Support
# -------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ECMWF Open Data Downloader (2026 Compatible)")
    parser.add_argument("--date", required=True, help="YYYYMMDD (e.g. 20260128)")
    parser.add_argument("--run", type=int, required=True, help="Run hour: 0, 6, 12, 18")
    parser.add_argument("--domain", required=True, help="Options: aifs025_single, aifs025_ensemble, ifs025, ifs025_ensemble, wam025, wam025_ensemble")
    parser.add_argument("--outdir", required=True, help="Directory to save GRIB files")
    parser.add_argument("--max-forecast-hour", type=int, help="Optional limit (e.g., 24)")
    args = parser.parse_args()

    run_date = dt.datetime.strptime(args.date, "%Y%m%d")
    run_full = run_date.replace(hour=args.run, tzinfo=dt.timezone.utc)

    download_ecmwf(
        domain=args.domain,
        output_dir=Path(args.outdir),
        run=run_full,
        max_forecast_hour=args.max_forecast_hour
    )