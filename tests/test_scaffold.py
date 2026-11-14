import pathlib
import unittest

class Scaffold(unittest.TestCase):
    def test_private_data_excluded(self):
        text=(pathlib.Path(__file__).resolve().parents[1]/".gitignore").read_text()
        for pattern in (".env", "*.sqlite*", "*.jsonl*", "data/"):
            self.assertIn(pattern,text)
