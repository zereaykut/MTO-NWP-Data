import unittest
from unittest.mock import patch, MagicMock
import datetime as dt
from pathlib import Path
import sys
import shutil

# Ensure src is in python path
sys.path.append(str(Path(__file__).parent.parent))

from src.components import gfs, ecmwf, icon, arpage

class TestMeteorologicalModels(unittest.TestCase):
    
    def setUp(self):
        """Create a temporary directory for test outputs."""
        self.test_dir = Path("test_output_temp")
        self.test_dir.mkdir(exist_ok=True)
        self.fixed_date = dt.datetime(2025, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)

    def tearDown(self):
        """Cleanup temporary directory."""
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    # ------------------------------------------------------------------------
    # GFS TESTS
    # ------------------------------------------------------------------------
    @patch('src.components.gfs.requests.Session')
    def test_gfs_download_calls(self, mock_session):
        """Test if GFS downloader constructs URLs and calls get."""
        # Setup mock
        mock_get = MagicMock()
        mock_get.status_code = 200
        mock_get.iter_content.return_value = [b"data"]
        mock_session.return_value.get.return_value.__enter__.return_value = mock_get

        # Run GFS download
        gfs.download_gfs(
            domain="gfs025",
            output_dir=self.test_dir,
            run=self.fixed_date,
            max_forecast_hour=0 # Only download forecast hour 0
        )

        # Assertions
        # 1. Check if directory creation was attempted (implicitly via success)
        # 2. Check if requests.get was called
        self.assertTrue(mock_session.called)
        
        # Verify URL structure for GFS 0.25
        # Expected: https://nomads.ncep.noaa.gov/.../gfs.20250101/00/atmos/gfs.t00z.pgrb2.0p25.f000
        calls = mock_session.return_value.get.call_args_list
        self.assertTrue(len(calls) > 0)
        url_used = calls[0][0][0]
        self.assertIn("gfs.20250101/00/atmos/gfs.t00z.pgrb2.0p25.f000", url_used)

    # ------------------------------------------------------------------------
    # ECMWF TESTS
    # ------------------------------------------------------------------------
    @patch('src.components.ecmwf.requests.Session')
    def test_ecmwf_download_calls(self, mock_session):
        """Test ECMWF URL construction."""
        mock_get = MagicMock()
        mock_get.status_code = 200
        mock_get.iter_content.return_value = [b"data"]
        mock_session.return_value.get.return_value.__enter__.return_value = mock_get

        ecmwf.download_ecmwf(
            domain="ifs04",
            output_dir=self.test_dir,
            run=self.fixed_date,
            max_forecast_hour=0
        )

        # Check URL
        calls = mock_session.return_value.get.call_args_list
        self.assertTrue(len(calls) > 0)
        url_used = calls[0][0][0]
        # ECMWF Open Data: 20250101/00z/ifs/0p4-beta/oper/20250101000000-0h-oper-fc.grib2
        self.assertIn("20250101/00z/ifs/0p4-beta/oper", url_used)
        self.assertIn("-0h-oper-fc.grib2", url_used)

    # ------------------------------------------------------------------------
    # ICON TESTS
    # ------------------------------------------------------------------------
    @patch('src.components.icon.requests.Session')
    def test_icon_download_calls(self, mock_session):
        """Test ICON download logic."""
        mock_get = MagicMock()
        mock_get.status_code = 200
        mock_get.iter_content.return_value = [b"icon_data"]
        mock_session.return_value.get.return_value.__enter__.return_value = mock_get

        icon.download_icon(
            domain="icon-eu",
            run=self.fixed_date,
            variables=["temperature_2m"],
            output_dir=self.test_dir,
            max_steps=1
        )

        calls = mock_session.return_value.get.call_args_list
        self.assertTrue(len(calls) > 0)
        url_used = calls[0][0][0]
        
        # Validate DWD URL structure
        # icon-eu_europe_regular-lat-lon_single-level_2025010100_000_t_2m.grib2.bz2
        self.assertIn("icon-eu_europe_regular-lat-lon", url_used)
        self.assertIn("t_2m", url_used)

    # ------------------------------------------------------------------------
    # ARPEGE TESTS
    # ------------------------------------------------------------------------
    @patch('src.components.arpage.requests.Session')
    def test_arpege_download_calls(self, mock_session):
        """Test ARPEGE logic."""
        mock_get = MagicMock()
        mock_get.status_code = 200
        mock_get.iter_content.return_value = [b"arpege_data"]
        mock_session.return_value.get.return_value.__enter__.return_value = mock_get

        arpage.download_arpege(
            domain="arpege_world",
            output_dir=self.test_dir,
            run=self.fixed_date,
            max_forecast_hour=0
        )

        calls = mock_session.return_value.get.call_args_list
        self.assertTrue(len(calls) > 0)
        url_used = calls[0][0][0]
        
        # Validate MeteoFrance URL structure
        # 2025-01-01T00:00Z/arpege/0.25/SP1/arpege__0.25__SP1__000H024H__2025-01-01T00:00Z.grib2
        self.assertIn("arpege/0.25/SP1", url_used)
        self.assertIn("000H024H", url_used)

    # ------------------------------------------------------------------------
    # ERROR HANDLING TESTS
    # ------------------------------------------------------------------------
    def test_invalid_domains(self):
        """Ensure invalid domains raise ValueError immediately."""
        with self.assertRaises(ValueError):
            gfs.download_gfs("fake_gfs", self.test_dir)
            
        with self.assertRaises(ValueError):
            # Using try/except because EcmwfDomain init raises the error
            ecmwf.download_ecmwf("fake_ecmwf", self.test_dir, self.fixed_date)

        with self.assertRaises(ValueError):
            icon.download_icon("fake_icon", self.fixed_date, [], self.test_dir)

        with self.assertRaises(ValueError):
            arpage.download_arpege("fake_arpege", self.test_dir)

if __name__ == "__main__":
    unittest.main()