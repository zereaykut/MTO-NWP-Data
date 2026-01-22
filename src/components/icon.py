#!/usr/bin/env python3
"""
ICON Downloader Component (DWD Open Data)
"""

import argparse
import datetime as dt
from pathlib import Path
from typing import List, Dict, Optional
import requests

# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------

ICON_DOMAINS: Dict[str, Dict[str, str]] = {
    "icon": {"path": "icon", "region": "global", "grid_type": "icosahedral"},
    "icon-eu": {"path": "icon-eu", "region": "europe", "grid_type": "regular-lat-lon"},
    "icon-d2": {"path": "icon-d2", "region": "germany", "grid_type": "regular-lat-lon"},
}

ICON_SURFACE_VARIABLES: Dict[str, Dict[str, object]] = {
    "temperature_2m": {"param": "t_2m", "cat": "single-level", "level": None},
    "precipitation": {"param": "tot_prec", "cat": "single-level", "level": None},
    "cloud_cover": {"param": "clct", "cat": "single-level", "level": None},
    "wind_u_component_10m": {"param": "u_10m", "cat": "single-level", "level": None},
    "wind_v_component_10m": {"param": "v_10m", "cat": "single-level", "level": None},
}

# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def get_download_forecast_steps(domain_key: str, run_hour: int) -> List[int]:
    if domain_key == "icon":
        if run_hour in (6, 18):
            return list(range(0, 79)) + list(range(81, 121, 3))
        else:
            return list(range(0, 79)) + list(range(81, 181, 3))
    
    if domain_key == "icon-eu":
        if run_hour % 6 == 0:
            return list(range(0, 79)) + list(range(81, 121, 3))
        return list(range(0, 31))

    if domain_key == "icon-d2":
        return list(range(0, 49))
        
    raise ValueError(f"Unsupported domain: {domain_key}")

def build_icon_url(domain_key: str, run_time: dt.datetime, step_hour: int, var_key: str) -> str:
    domain_conf = ICON_DOMAINS[domain_key]
    var_conf = ICON_SURFACE_VARIABLES[var_key]
    
    domain_path = domain_conf["path"]
    region = domain_conf["region"]
    grid = domain_conf["grid_type"]
    param = var_conf["param"]
    cat = var_conf["cat"]
    
    # Example: icon-eu_europe_regular-lat-lon_single-level_2025120112_005_t_2m.grib2.bz2
    domain_prefix = f"{domain_path}_{region}"
    date_str = run_time.strftime("%Y%m%d%H")
    step_str = f"{step_hour:03d}"
    
    filename = f"{domain_prefix}_{grid}_{cat}_{date_str}_{step_str}_{param}.grib2.bz2"
    
    # URL structure: .../grib/HH/param/filename
    server = f"https://opendata.dwd.de/weather/nwp/{domain_path}/grib/{run_time.hour:02d}/{param}"
    return f"{server}/{filename}"

# -------------------------------------------------------------------------
# Core Logic
# -------------------------------------------------------------------------

def download_icon(
    domain: str,
    run: dt.datetime,
    variables: List[str],
    output_dir: Path,
    max_steps: Optional[int] = None
):
    if domain not in ICON_DOMAINS:
        raise ValueError(f"Unknown domain {domain}")
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    steps = get_download_forecast_steps(domain, run.hour)
    if max_steps:
        steps = steps[:max_steps]
        
    print(f"[ICON] Domain={domain} Run={run.strftime('%Y%m%d%H')} Vars={len(variables)}")
    
    session = requests.Session()
    
    for var in variables:
        if var not in ICON_SURFACE_VARIABLES:
            print(f"  [WARN] Skipping unknown variable: {var}")
            continue
            
        for step in steps:
            url = build_icon_url(domain, run, step, var)
            fname = url.split("/")[-1]
            out_path = output_dir / fname
            
            if out_path.exists():
                print(f"  [SKIP] {fname}")
                continue
                
            print(f"  [DOWN] {fname}")
            try:
                with session.get(url, stream=True, timeout=20) as r:
                    r.raise_for_status()
                    with open(out_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
            except Exception as e:
                print(f"  [ERR] {e}")

# -------------------------------------------------------------------------
# CLI Support
# -------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--domain", required=True, choices=ICON_DOMAINS.keys())
    parser.add_argument("--run", required=True, help="YYYYMMDDHH")
    parser.add_argument("--variables", required=True, help="comma separated")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    
    r_dt = dt.datetime.strptime(args.run, "%Y%m%d%H").replace(tzinfo=dt.timezone.utc)
    vars_list = [v.strip() for v in args.variables.split(",") if v.strip()]
    
    download_icon(args.domain, r_dt, vars_list, Path(args.out))