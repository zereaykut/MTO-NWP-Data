import datetime as dt
from pathlib import Path
import sys

# Ensure src is in the python path
sys.path.append(str(Path(__file__).parent))

from src.components.gfs import download_gfs
from src.components.ecmwf import download_ecmwf
from src.components.icon import download_icon
from src.components.arpage import download_arpege

def main():
    # Define a base output directory
    base_dir = Path("weather_data")
    base_dir.mkdir(exist_ok=True)
    
    # Use the current UTC time to approximate the latest runs
    now = dt.datetime.now(dt.timezone.utc)
    
    print(f"--- Starting Meteorological Data Downloader ---")
    print(f"Reference Time (UTC): {now.isoformat()}")
    print("-" * 50)

    # ---------------------------------------------------------
    # 1. GFS Example
    # ---------------------------------------------------------
    print("\n>>> Fetching GFS (0.25 Degree)...")
    try:
        # GFS runs every 6 hours (00, 06, 12, 18). 
        # We'll let the module auto-detect the latest run by passing run=None.
        download_gfs(
            domain="gfs025",
            output_dir=base_dir / "gfs",
            run=None,                # Auto-detect latest
            max_forecast_hour=3,     # Just download first 3 hours for demo
            use_aws=False            # Use NOMADS (faster for recent data)
        )
    except Exception as e:
        print(f"GFS Error: {e}")

    # ---------------------------------------------------------
    # 2. ECMWF Example
    # ---------------------------------------------------------
    print("\n>>> Fetching ECMWF (IFS 0.25°)...")
    try:
        # ECMWF Open Data runs: 00, 06, 12, 18 UTC.
        # We calculate the latest available run.
        # (Subtracting 7-8 hours is usually safer to ensure upload completion)
        safe_run_time = now - dt.timedelta(hours=7)
        floored_hour = (safe_run_time.hour // 6) * 6
        ecmwf_run = safe_run_time.replace(hour=floored_hour, minute=0, second=0, microsecond=0)

        download_ecmwf(
            domain="ifs025",        # Changed from 'ifs04' to match your 0.25° links
            output_dir=base_dir / "ecmwf",
            run=ecmwf_run,
            max_forecast_hour=3     # Keep it small for testing
        )
    except Exception as e:
        print(f"ECMWF Error: {e}")

    # ---------------------------------------------------------
    # 3. ICON Example
    # ---------------------------------------------------------
    print("\n>>> Fetching ICON (Global)...")
    try:
        # Download Surface Data
        download_icon(
            run=dt.datetime(2026, 1, 31, 0, 0, 0, tzinfo=dt.timezone.utc),
            variables=["t_2m", "u_10m", "v_10m", "tot_prec", "pmsl"],
            output_dir=Path("./weather_data/icon/single-level"),
            steps=[0, 1, 2, 3]
        )

        # Download 850hPa Temperature
        download_icon(
            run=dt.datetime(2026, 1, 31, 0, 0, 0, tzinfo=dt.timezone.utc),
            variables=["t"],
            output_dir=Path("./weather_data/icon/pressure-level-temperature"),
            steps=[0, 3, 6],
            level_type="pressure-level",
            level=850   # 1000, 950, 850, 700, 500, 250, 200
        )
    except Exception as e:
        print(f"ICON Error: {e}")

    # ---------------------------------------------------------
    # 4. ARPEGE Example
    # ---------------------------------------------------------
    print("\n>>> Fetching ARPEGE (Europe)...")
    try:
        # Let the module guess the last run
        download_arpege(
            domain="arpege_europe",
            output_dir=base_dir / "arpege",
            run=None,
            max_forecast_hour=3
        )
    except Exception as e:
        print(f"ARPEGE Error: {e}")

    print("\n" + "-" * 50)
    print("Download process completed.")

if __name__ == "__main__":
    main()