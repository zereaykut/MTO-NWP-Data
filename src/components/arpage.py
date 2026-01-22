#!/usr/bin/env python3
"""
MeteoFrance ARPEGE/AROME Downloader Component
"""

import argparse
import datetime as dt
from pathlib import Path
from typing import Optional
import requests

# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------

DOMAINS = {
    "arpege_europe": {
        "family": "arpege",
        "grid_api": "0.1",
        "grid_res": "01",
        "package_times": [
            "000H012H", "013H024H", "025H036H", "037H048H",
            "049H060H", "061H072H", "073H084H", "085H096H", "097H102H"
        ],
        "update_interval_hours": 6,
    },
    "arpege_world": {
        "family": "arpege",
        "grid_api": "0.25",
        "grid_res": "025",
        "package_times": [
            "000H024H", "025H048H", "049H072H", "073H102H"
        ],
        "update_interval_hours": 6,
    },
}

# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def guess_last_run(domain: str) -> dt.datetime:
    info = DOMAINS[domain]
    interval = info["update_interval_hours"]
    now = dt.datetime.utcnow()
    t = now - dt.timedelta(hours=2) # Latency buffer
    
    floored_hour = (t.hour // interval) * interval
    return t.replace(hour=floored_hour, minute=0, second=0, microsecond=0)

def build_gov_url(domain: str, run: dt.datetime, package: str, package_time: str) -> str:
    info = DOMAINS[domain]
    family = info["family"]
    grid_res = info["grid_res"]
    
    # Format: YYYY-MM-DDTHH:MM
    run_iso_short = run.strftime("%Y-%m-%dT%H:%M")
    
    # URL construction
    base = "https://object.data.gouv.fr/meteofrance-pnt/pnt"
    path = f"{run_iso_short}:00Z/{family}/{grid_res}/{package}/{family}__{grid_res}__{package}__{package_time}__{run_iso_short}:00Z.grib2"
    return f"{base}/{path}"

# -------------------------------------------------------------------------
# Core Logic
# -------------------------------------------------------------------------

def download_arpege(
    domain: str,
    output_dir: Path,
    run: Optional[dt.datetime] = None,
    max_forecast_hour: Optional[int] = None
):
    if domain not in DOMAINS:
        raise ValueError(f"Unknown domain: {domain}")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if run is None:
        run = guess_last_run(domain)
    
    print(f"[ARPAGE] Domain={domain} Run={run.isoformat()}")
    
    info = DOMAINS[domain]
    # Default packages
    packages = ["SP1", "SP2", "HP1", "IP1"] 
    package_times = info["package_times"]

    session = requests.Session()

    for p_time in package_times:
        # Check max forecast hour logic if needed (parsing "000H012H")
        try:
            start_hour = int(p_time.split("H")[0])
            if max_forecast_hour is not None and start_hour > max_forecast_hour:
                continue
        except:
            pass

        for pkg in packages:
            url = build_gov_url(domain, run, pkg, p_time)
            # Output filename
            fname = f"{domain}_{run.strftime('%Y%m%d%H')}_{pkg}_{p_time}.grib2"
            out_path = output_dir / fname

            if out_path.exists():
                print(f"  [SKIP] {fname}")
                continue
            
            print(f"  [DOWN] {fname}")
            try:
                with session.get(url, stream=True, timeout=30) as r:
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
    parser.add_argument("--domain", required=True, choices=DOMAINS.keys())
    parser.add_argument("--run", help="YYYY-MM-DDTHH")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    r_dt = None
    if args.run:
        # Simple parser for CLI
        r_dt = dt.datetime.strptime(args.run, "%Y-%m-%dT%H")

    download_arpege(args.domain, Path(args.output), run=r_dt)