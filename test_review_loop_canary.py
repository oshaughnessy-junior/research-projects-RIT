import unittest

from review_loop_canary import parity_sign


class ReviewLoopCanaryTest(unittest.TestCase):
    def test_parity_sign(self):
        self.assertEqual(parity_sign(2), 1)
        self.assertEqual(parity_sign(3), -1)


if __name__ == "__main__":
    unittest.main()
