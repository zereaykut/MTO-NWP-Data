#!/usr/bin/env python3
"""
ICON-EU Downloader (DWD Open Data)
Target: https://opendata.dwd.de/weather/nwp/icon-eu/grib/
"""

import argparse
import bz2
import datetime as dt
import os
from pathlib import Path
from typing import List, Optional
import requests

# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------

# Base URL for DWD Open Data ICON-EU GRIB
BASE_URL = "https://opendata.dwd.de/weather/nwp/icon-eu/grib"

# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def get_latest_run(now: Optional[dt.datetime] = None) -> dt.datetime:
    """
    Calculates the latest available ICON-EU run.
    Runs are at 00, 06, 12, 18 UTC.
    Data is typically available ~3.5 hours after run time.
    """
    if now is None:
        now = dt.datetime.now(dt.timezone.utc)
    
    # Heuristic: Go back 4 hours to be safe, then floor to 6h
    safe_time = now - dt.timedelta(hours=4)
    hour = (safe_time.hour // 6) * 6
    return safe_time.replace(hour=hour, minute=0, second=0, microsecond=0)

def build_dwd_url(
    run: dt.datetime,
    var: str,
    step: int,
    level_type: str = "single-level",
    level: Optional[int] = None
) -> str:
    """
    Constructs the DWD Open Data URL.
    
    Format:
    Base/{run_hour}/{var_lower}/icon-eu_europe_regular-lat-lon_{level_type}_{date}{run}_{step}_{level?}_{VAR_UPPER}.grib2.bz2
    """
    run_hour = f"{run.hour:02d}"
    date_str = run.strftime("%Y%m%d")
    date_run = f"{date_str}{run_hour}"
    
    # Variable directory is usually lowercase (e.g., t_2m)
    var_dir = var.lower()
    
    # Variable in filename is usually uppercase (e.g., T_2M)
    var_file = var.upper()
    
    step_str = f"{step:03d}"
    
    # Construct filename parts
    # Prefix: icon-eu_europe_regular-lat-lon
    prefix = "icon-eu_europe_regular-lat-lon"
    
    if level is not None:
        # Pressure/Model level format: ..._pressure-level_2026013100_000_1000_T.grib2.bz2
        # Note: Level comes BEFORE variable in filename
        filename = f"{prefix}_{level_type}_{date_run}_{step_str}_{level}_{var_file}.grib2.bz2"
    else:
        # Single level format: ..._single-level_2026013100_000_T_2M.grib2.bz2
        filename = f"{prefix}_{level_type}_{date_run}_{step_str}_{var_file}.grib2.bz2"

    url = f"{BASE_URL}/{run_hour}/{var_dir}/{filename}"
    return url

# -------------------------------------------------------------------------
# Core Logic
# -------------------------------------------------------------------------

def download_icon(
    run: dt.datetime,
    variables: List[str],
    output_dir: Path,
    steps: List[int],
    level_type: str = "single-level",
    level: Optional[int] = None
):
    """
    Downloads and decompresses ICON data.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()

    print(f"[ICON] Target: {run.isoformat()} | Vars: {variables}")
    print(f"[ICON] Steps:  {steps}")
    
    if level:
        print(f"[ICON] Level:  {level} ({level_type})")

    for var in variables:
        for step in steps:
            url = build_dwd_url(run, var, step, level_type, level)
            
            # Output filename (remove .bz2 extension for the local file)
            # We standardize the local name to be simpler:
            # icon-eu_YYYYMMDDHH_step_var[_level].grib2
            if level:
                fname = f"icon-eu_{run.strftime('%Y%m%d%H')}_{step:03d}_{var}_{level}.grib2"
            else:
                fname = f"icon-eu_{run.strftime('%Y%m%d%H')}_{step:03d}_{var}.grib2"
                
            out_path = output_dir / fname

            if out_path.exists():
                print(f"  [SKIP] {fname}")
                continue

            print(f"  [DOWN] {fname}") #  <- {url}
            
            try:
                with session.get(url, stream=True, timeout=30) as r:
                    if r.status_code == 404:
                        print(f"  [404 ] Not Found: {url}")
                        continue
                    r.raise_for_status()
                    
                    # DWD sends .bz2. We decompress on the fly.
                    decompressor = bz2.BZ2Decompressor()
                    
                    with open(out_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=16384):
                            if chunk:
                                try:
                                    data = decompressor.decompress(chunk)
                                    f.write(data)
                                except OSError as e:
                                    # Handle case where stream might end or be corrupt
                                    print(f"  [ERR ] Decompression error: {e}")
                                    break
                                    
            except Exception as e:
                print(f"  [ERR ] Failed: {e}")
                if out_path.exists():
                    out_path.unlink()

# -------------------------------------------------------------------------
# CLI Support
# -------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ICON-EU Downloader (DWD)")
    parser.add_argument("--run", type=str, help="YYYYMMDDHH (e.g., 2026013100)")
    parser.add_argument("--vars", required=True, help="Comma separated (e.g., t_2m,u_10m,tot_prec)")
    parser.add_argument("--outdir", required=True, help="Output directory")
    parser.add_argument("--steps", type=str, default="0-72-3", help="Start-End-Step (e.g., 0-12-1 or 0,1,2,3)")
    
    # Advanced Options for 3D variables
    parser.add_argument("--level-type", default="single-level", choices=["single-level", "pressure-level", "model-level"])
    parser.add_argument("--level", type=int, help="Level value (e.g., 1000, 850, 500). Required for pressure-level.")

    args = parser.parse_args()

    # Parse Run Time
    if args.run:
        run_dt = dt.datetime.strptime(args.run, "%Y%m%d%H").replace(tzinfo=dt.timezone.utc)
    else:
        run_dt = get_latest_run()

    # Parse Variables
    var_list = [v.strip() for v in args.vars.split(",") if v.strip()]

    # Parse Steps
    if "-" in args.steps:
        start, end, step_sz = map(int, args.steps.split("-"))
        step_list = list(range(start, end + 1, step_sz))
    else:
        step_list = [int(x) for x in args.steps.split(",")]

    download_icon(
        run=run_dt,
        variables=var_list,
        output_dir=Path(args.outdir),
        steps=step_list,
        level_type=args.level_type,
        level=args.level
    )