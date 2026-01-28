# MTO-NWP-Data

**MTO-NWP-Data** is a modular Python-based tool designed to automate the retrieval of Numerical Weather Prediction (NWP) data from major global meteorological centers.

It provides a unified interface and command-line tools to download GRIB2 data from **NOAA (GFS/GEFS)**, **ECMWF (IFS/AIFS)**, **DWD (ICON)**, and **Meteo-France (ARPEGE)**.

## 🚀 Features

* **Multi-Model Support:** One codebase to access GFS, ECMWF Open Data, ICON, and ARPEGE.
* **Modular Design:** Each downloader (`gfs`, `ecmwf`, `icon`, `arpage`) functions as a standalone component or an importable library.
* **Flexible Access:**
    * **GFS:** Supports both NOMADS (live) and AWS S3 (archive) sources.
    * **ECMWF:** Access to the new Open Data API, including the AIFS (Artificial Intelligence Forecasting System).
    * **ICON:** Granular variable selection to save bandwidth.
* **Smart Run Detection:** Auto-detects the latest available model run times to ensure data freshness.

## 📂 Directory Structure

```text
MTO-NWP-Data/
├── app.py                  # Main demonstration/runner script
├── requirements.txt        # Python dependencies
├── README.md               # Project documentation
├── src/
│   ├── components/         # Core downloader modules
│   │   ├── arpage.py       # Meteo-France ARPEGE downloader
│   │   ├── ecmwf.py        # ECMWF Open Data downloader
│   │   ├── gfs.py          # NOAA GFS/GEFS downloader
│   │   ├── icon.py         # DWD ICON downloader
│   │   └── __init__.py
│   └── __init__.py
└── tests/
    └── test_models.py      # Unit tests