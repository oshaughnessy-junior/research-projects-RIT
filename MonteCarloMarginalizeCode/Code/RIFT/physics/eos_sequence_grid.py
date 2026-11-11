"""Fixed NMB sequence hypotheses in the Hyperpipe numeric grid protocol."""

import numpy as np

from RIFT.physics import EOSManager


class SequenceGrid:
    """Load a sequence once; retain the grid's native, unsorted sequence indices."""

    def __init__(self, grid_file, sequence_file):
        with open(grid_file) as stream:
            header = stream.readline()
        if not header.startswith("#"):
            raise ValueError("NMB EOS grid requires a #-prefixed column header")
        self.columns = header.lstrip("#").split()
        if self.columns.count("sequence_index") != 1 or self.columns.index("sequence_index") < 2:
            raise ValueError("NMB EOS grid requires one sequence_index column after lnL sigma_lnL")
        rows = np.atleast_2d(np.loadtxt(grid_file))
        if rows.shape[1] != len(self.columns):
            raise ValueError("NMB EOS grid header and numeric columns disagree")
        self.index_column = self.columns.index("sequence_index")
        self.sequence_file = sequence_file
        # Missing files and malformed producer schemas are configuration failures.
        self.sequence = EOSManager.EOSSequenceFromFile(
            fname=sequence_file, load_ns=True, no_sort=True)
        if not isinstance(self.sequence, EOSManager.EOSSequenceNMB):
            raise ValueError("--eos-param nmbseq requires an NMB tabular or PCA sequence")

    def eos_for_row(self, row):
        value = float(row[self.index_column])
        if not np.isfinite(value) or value < 0 or not value.is_integer():
            raise ValueError("sequence_index must be a finite nonnegative integer")
        return EOSManager.EOSSequenceSingleIndex(
            fname=self.sequence_file, index=int(value), sequence=self.sequence)
