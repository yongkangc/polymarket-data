"""
Unit tests for updown_pipeline.market_discovery module
"""
import unittest
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from updown_pipeline.market_discovery import extract_duration


class TestExtractDuration(unittest.TestCase):
    """Test cases for extract_duration function"""

    def test_5m_duration_standard_format(self):
        """Test extraction of 5m duration from standard slug format"""
        self.assertEqual(extract_duration('btc-updown-5m-1766991300'), '5m')
        self.assertEqual(extract_duration('eth-updown-5m-1234567890'), '5m')
        self.assertEqual(extract_duration('sol-updown-5m-9876543210'), '5m')

    def test_15m_duration_standard_format(self):
        """Test extraction of 15m duration from standard slug format"""
        self.assertEqual(extract_duration('sol-updown-15m-1766941200'), '15m')
        self.assertEqual(extract_duration('btc-updown-15m-1766935800'), '15m')
        self.assertEqual(extract_duration('eth-updown-15m-1234567890'), '15m')

    def test_1h_duration_standard_format(self):
        """Test extraction of 1h duration from standard slug format"""
        self.assertEqual(extract_duration('eth-updown-1h-1766973000'), '1h')
        self.assertEqual(extract_duration('btc-updown-1h-1234567890'), '1h')
        self.assertEqual(extract_duration('sol-updown-1h-9876543210'), '1h')

    def test_duration_with_spaces(self):
        """Test extraction from formats with spaces (e.g., '5 min')"""
        self.assertEqual(extract_duration('btc 5 min market'), '5m')
        self.assertEqual(extract_duration('eth 15 min market'), '15m')
        self.assertEqual(extract_duration('sol 1 hour market'), '1h')

    def test_case_insensitivity(self):
        """Test that matching is case-insensitive"""
        self.assertEqual(extract_duration('BTC-UPDOWN-5M-1234567890'), '5m')
        self.assertEqual(extract_duration('SOL-UPDOWN-15M-1234567890'), '15m')
        self.assertEqual(extract_duration('ETH-UPDOWN-1H-1234567890'), '1h')

    def test_no_duration_match(self):
        """Test that None is returned when no duration pattern matches"""
        self.assertIsNone(extract_duration('btc-updown-test'))
        self.assertIsNone(extract_duration('random-slug'))
        self.assertIsNone(extract_duration(''))
        self.assertIsNone(extract_duration('btc-updown-2h-1234567890'))  # 2h not supported

    def test_no_false_positives_15m_vs_5m(self):
        """Test that 15m is not confused with 5m (the original bug)"""
        # This was the bug: "5m" substring matched within "15m"
        result = extract_duration('sol-updown-15m-1766941200')
        self.assertEqual(result, '15m')
        self.assertNotEqual(result, '5m')

    def test_mixed_content_with_duration(self):
        """Test extraction from slugs with additional content"""
        self.assertEqual(extract_duration('bitcoin-updown-5m-test-market'), '5m')
        self.assertEqual(extract_duration('ethereum-special-15m-market-1234'), '15m')
        self.assertEqual(extract_duration('solana-test-1h-market'), '1h')


if __name__ == '__main__':
    unittest.main()
