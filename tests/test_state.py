import tempfile
import unittest
from pathlib import Path

from hubspot_dlt.state import ControlStore


class ControlStoreTests(unittest.TestCase):
    def test_pause_and_resume_persist_across_instances(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Path(temporary_directory) / "state.sqlite"
            ControlStore(database).set_paused(True)

            self.assertTrue(ControlStore(database).is_paused())

            ControlStore(database).set_paused(False)
            self.assertFalse(ControlStore(database).is_paused())

    def test_page_progress_is_persisted(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            state = ControlStore(Path(temporary_directory) / "state.sqlite")
            state.record_page("deals", "next-page", "2026-10-10T12:00:00+00:00")

            status = state.status()
            self.assertFalse(status["paused"])
            self.assertEqual(status["resources"][0]["resource_name"], "deals")
            self.assertEqual(status["resources"][0]["next_offset"], "next-page")
            self.assertEqual(
                status["resources"][0]["cursor_timestamp"],
                "2026-10-10T12:00:00+00:00",
            )


if __name__ == "__main__":
    unittest.main()