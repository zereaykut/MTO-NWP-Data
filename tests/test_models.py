import unittest
import datetime as dt
from src.components.gfs import GFSDownloader
from src.components.ecmwf import ECMWFDownloader
from src.components.icon import IconDownloader

class TestWeatherModels(unittest.TestCase):
    def setUp(self):
        self.test_date = dt.datetime(2025, 1, 1, 0, 0)

    def test_gfs_url_logic(self):
        dl = GFSDownloader(output_dir="./tmp")
        urls = dl.build_urls(self.test_date, 24)
        self.assertIn("20250101", urls[0])
        self.assertIn("f024", urls[0])

    def test_ecmwf_steps(self):
        dl = ECMWFDownloader(output_dir="./tmp")
        steps = dl.get_forecast_steps()
        self.assertEqual(steps[0], 0)
        self.assertIn(144, steps)

    def test_icon_eu_steps(self):
        dl = IconDownloader(output_dir="./tmp")
        steps = dl.get_forecast_steps("icon-eu")
        self.assertEqual(len(steps), 93) # Expected steps for icon-eu

if __name__ == "__main__":
    unittest.main()