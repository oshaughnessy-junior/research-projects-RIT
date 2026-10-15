import unittest
from distill_diagnostics import original_pair_coverage,semigroup_counterexample


class DistillDiagnosticTests(unittest.TestCase):
    def test_original_endpoint_coverage(self):
        r=original_pair_coverage()
        self.assertEqual(r['full_interval_probability'],1/1024)
        self.assertEqual(r['expected_full_interval_rows_in_6000_batches'],750.)
        self.assertEqual(r['mean_span'],.2734375)

    def test_passing_local_and_self_consistency_does_not_imply_teacher_accuracy(self):
        r=semigroup_counterexample()
        self.assertLessEqual(r['semigroup_rms'],.1)
        self.assertLessEqual(r['diagonal_rms'],.2)
        self.assertGreater(r['one_step_rms'],.15)


if __name__=='__main__':unittest.main(verbosity=2)
