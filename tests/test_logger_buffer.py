import logging
import unittest
from logger_buffer import InMemoryLogHandler


class TestInMemoryLogHandler(unittest.TestCase):
    def setUp(self):
        self.handler = InMemoryLogHandler(capacity=5)
        self.logger = logging.getLogger("test_logger")
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(self.handler)

    def tearDown(self):
        self.logger.removeHandler(self.handler)

    def test_emit_and_retrieve_logs(self):
        self.logger.info("Info message 1")
        self.logger.warning("Warning message 2")
        self.logger.error("Error message 3")

        logs = self.handler.get_logs(limit=10)
        self.assertEqual(len(logs), 3)
        # Newest first
        self.assertEqual(logs[0]["message"], "Error message 3")
        self.assertEqual(logs[0]["level"], "ERROR")
        self.assertEqual(logs[1]["message"], "Warning message 2")
        self.assertEqual(logs[2]["message"], "Info message 1")

    def test_ring_buffer_capacity(self):
        for i in range(10):
            self.logger.info(f"Msg {i}")

        logs = self.handler.get_logs(limit=10)
        self.assertEqual(len(logs), 5)  # capacity is 5
        self.assertEqual(logs[0]["message"], "Msg 9")
        self.assertEqual(logs[-1]["message"], "Msg 5")

    def test_level_filtering(self):
        self.logger.debug("Debug msg")
        self.logger.info("Info msg")
        self.logger.warning("Warning msg")
        self.logger.error("Error msg")

        errors_only = self.handler.get_errors()
        self.assertEqual(len(errors_only), 2)
        levels = [e["level"] for e in errors_only]
        self.assertIn("WARNING", levels)
        self.assertIn("ERROR", levels)
        self.assertNotIn("INFO", levels)
        self.assertNotIn("DEBUG", levels)

    def test_logger_name_filtering(self):
        other_logger = logging.getLogger("other_service")
        other_logger.setLevel(logging.INFO)
        other_logger.addHandler(self.handler)

        self.logger.info("From test")
        other_logger.info("From other")

        filtered = self.handler.get_logs(logger_name="other")
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["logger"], "other_service")

        other_logger.removeHandler(self.handler)


if __name__ == "__main__":
    unittest.main()
