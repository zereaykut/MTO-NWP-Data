#!/usr/bin/env python3
"""
Simple GFS / GEFS / GFS-Wave downloader component.
"""

import argparse
import datetime as dt
import os
from pathlib import Path
from typing import List, Optional
import requests

# -------------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------------

DOMAINS = [
    "gfs013", "gfs025", "gfs025_ens", "gfs05_ens",
    "gfswave025", "gfswave016", "gfswave025_ens",
    "nam_conus", "hrrr_conus", "hrrr_conus_15min",
]

ENSEMBLE_MEMBER_COUNT = {
    "gfs05_ens": 31,
    "gfs025_ens": 31,
    "gfswave025_ens": 31,
}

# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def floor_to_6h(dt_utc: dt.datetime) -> dt.datetime:
    hour = (dt_utc.hour // 6) * 6
    return dt_utc.replace(hour=hour, minute=0, second=0, microsecond=0)

def last_run(domain: str, now: Optional[dt.datetime] = None) -> dt.datetime:
    if now is None:
        now = dt.datetime.now(dt.timezone.utc)
    
    if domain in ("gfs05_ens", "gfs025_ens", "gfswave025_ens",
                  "gfs013", "gfs025", "gfswave025", "gfswave016"):
        t = now - dt.timedelta(hours=4) # Adjusted buffer slightly
        return floor_to_6h(t)
    elif domain == "nam_conus":
        t = now - dt.timedelta(hours=2)
        return floor_to_6h(t)
    elif domain in ("hrrr_conus", "hrrr_conus_15min"):
        return now.replace(minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"Unknown domain for last_run: {domain}")

def forecast_hours(domain: str, run_hour: int, second_flush: bool) -> List[int]:
    if domain == "gfs05_ens":
        if second_flush:
            return list(range(390, 841, 6))
        return list(range(0, 240, 3)) + list(range(240, 385, 6))

    if domain in ("gfs025_ens", "gfswave025_ens"):
        return list(range(0, 241, 3))

    if domain in ("gfs013", "gfs025", "gfswave025", "gfswave016"):
        return list(range(0, 120, 1)) + list(range(120, 385, 3))

    if domain == "nam_conus":
        return list(range(0, 61))

    if domain == "hrrr_conus":
        if run_hour % 6 == 0:
            return list(range(0, 49))
        else:
            return list(range(0, 19))

    if domain == "hrrr_conus_15min":
        return list(range(0, 18 * 4 + 1))

    raise ValueError(f"Unknown domain for forecast_hours: {domain}")

def build_gfs_urls(domain: str, run_dt: dt.datetime, forecast_hour: int, member: int, use_aws: bool) -> List[str]:
    yyyymmdd = run_dt.strftime("%Y%m%d")
    hh = run_dt.strftime("%H")
    fHHH = f"{forecast_hour:03d}"

    gfsAws = "https://noaa-gfs-bdp-pds.s3.amazonaws.com/"
    gfsNomads = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"
    gefsAws = "https://noaa-gefs-pds.s3.amazonaws.com/"
    gefsNomads = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/"
    hrrrNomads = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/"
    hrrrAws = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com/"

    # Simple logic: fallback to AWS if requested or data is old
    now = dt.datetime.now(dt.timezone.utc)
    age_sec = (now - run_dt).total_seconds()
    use_archive = use_aws or (age_sec > 36 * 3600)

    gfs_server = gfsAws if use_archive else gfsNomads
    gefs_server = gefsAws if use_archive else gefsNomads
    hrrr_server = hrrrAws if use_archive else hrrrNomads

    # --- GEFS ---
    if domain == "gfs05_ens":
        member_str = "gec00" if member == 0 else f"gep{member:02d}"
        path = f"gefs.{yyyymmdd}/{hh}/atmos/pgrb2ap5/{member_str}.t{hh}z.pgrb2a.0p50.f{fHHH}"
        return [gefs_server + path]

    if domain == "gfs025_ens":
        member_str = "gec00" if member == 0 else f"gep{member:02d}"
        path = f"gefs.{yyyymmdd}/{hh}/atmos/pgrb2b25/{member_str}.t{hh}z.pgrb2b.0p25.f{fHHH}"
        return [gefs_server + path]

    if domain == "gfswave025_ens":
        member_str = "gec00" if member == 0 else f"gep{member:02d}"
        path = f"gefs.{yyyymmdd}/{hh}/wave/gridded/{member_str}.t{hh}z.global.0p25.f{fHHH}.grib2"
        return [gefs_server + path]

    # --- GFS Deterministic ---
    if domain == "gfs025":
        path = f"gfs.{yyyymmdd}/{hh}/atmos/gfs.t{hh}z.pgrb2.0p25.f{fHHH}"
        return [gfs_server + path]
    
    if domain == "gfs013":
        path = f"gfs.{yyyymmdd}/{hh}/atmos/gfs.t{hh}z.pgrb2.0p13.f{fHHH}"
        return [gfs_server + path]
    
    if domain == "gfswave025":
        path = f"gfs.{yyyymmdd}/{hh}/wave/gridded/gfswave.t{hh}z.global.0p25.f{fHHH}.grib2"
        return [gfs_server + path]

    if domain == "gfswave016":
        path = f"gfs.{yyyymmdd}/{hh}/wave/gridded/gfswave.t{hh}z.global.0p16.f{fHHH}.grib2"
        return [gfs_server + path]

    # --- HRRR / NAM ---
    if domain == "hrrr_conus":
        path = f"hrrr.{yyyymmdd}/conus/hrrr.t{hh}z.wrfsfcf{fHHH}.grib2"
        return [hrrr_server + path]

    return []

# -------------------------------------------------------------------------
# Core Logic
# -------------------------------------------------------------------------

def download_gfs(
    domain: str,
    output_dir: Path,
    run: Optional[dt.datetime] = None,
    max_forecast_hour: Optional[int] = None,
    use_aws: bool = False,
    second_flush: bool = False,
    max_members: Optional[int] = None
):
    """
    Main entry point for downloading GFS-family data.
    """
    if domain not in DOMAINS:
        raise ValueError(f"Invalid domain: {domain}")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Resolve run time
    if run is None:
        run_dt = last_run(domain)
    else:
        # Ensure UTC
        if run.tzinfo is None:
            run_dt = run.replace(tzinfo=dt.timezone.utc)
        else:
            run_dt = run.astimezone(dt.timezone.utc)

    yyyymmddhh = run_dt.strftime("%Y%m%d%H")
    print(f"[GFS] Domain={domain} Run={yyyymmddhh} Output={output_dir}")

    # Forecast hours
    fhours = forecast_hours(domain, run_dt.hour, second_flush)
    if max_forecast_hour is not None:
        fhours = [h for h in fhours if h <= max_forecast_hour]

    # Members
    if domain in ENSEMBLE_MEMBER_COUNT:
        n_members = ENSEMBLE_MEMBER_COUNT[domain]
        if max_members is not None:
            n_members = min(n_members, max_members)
        members = list(range(n_members))
    else:
        members = [0]

    session = requests.Session()

    for member in members:
        for fhour in fhours:
            urls = build_gfs_urls(domain, run_dt, fhour, member, use_aws)
            
            for url in urls:
                # Naming: domain_YYYYMMDDHH_memXX_fXXX.grib2
                fname = f"{domain}_{yyyymmddhh}_m{member:02d}_f{fhour:03d}.grib2"
                out_path = output_dir / fname

                if out_path.exists():
                    print(f"  [SKIP] {fname}")
                    continue
                
                print(f"  [DOWN] {fname} <- {url}")
                try:
                    with session.get(url, stream=True, timeout=10) as r:
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
    parser = argparse.ArgumentParser(description="GFS/GEFS Downloader")
    parser.add_argument("domain", choices=DOMAINS, help="Model domain")
    parser.add_argument("--run", type=str, help="YYYYMMDDHH format. Default: auto")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    parser.add_argument("--max-forecast-hour", type=int, help="Limit forecast hours")
    parser.add_argument("--use-aws", action="store_true", help="Force AWS S3")
    args = parser.parse_args()

    r_dt = None
    if args.run:
        r_dt = dt.datetime.strptime(args.run, "%Y%m%d%H").replace(tzinfo=dt.timezone.utc)

    download_gfs(
        domain=args.domain,
        output_dir=Path(args.output_dir),
        run=r_dt,
        max_forecast_hour=args.max_forecast_hour,
        use_aws=args.use_aws
    )