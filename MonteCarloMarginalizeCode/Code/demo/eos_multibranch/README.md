# Multi-branch EOS marginalization

Use a native NMB `nmbackend.nss/1` or `nmbackend.pca/1` sequence. The numeric
EOS grid names each producer-native sequence index. For example:

```text
# lnL sigma_lnL sequence_index
0 0 0
0 0 1
0 0 3
```

Each row is a fixed EOS hypothesis. Central mode retains all of that EOS's
stable branches, including overlapping masses. Its default measure is mass
on every stable branch. A component on no stable branch has zero weight.

For an event likelihood grid with masses, spins and tides, put the following
in that event's Hyperpipe `marg-list[].args`. Set `exe` to the CIP executable
and `event-file` to the event's grid. Use source-mass integration bounds
appropriate to that event and keep its fixed redshift explicit.

```text
--input-tides --parameter mc --parameter delta_mc
--parameter-implied LambdaTilde --fit-method quadratic
--mc-range '[1.55,1.70]' --eta-range '[0.2475,0.25]'
--source-redshift 0 --eos-param nmbseq
--eos-sequence-file /shared/eos/sequence.h5
--eos-affine-parameter central --eos-branch-prior mass-per-branch
--n-max 100000 --n-eff 1000 --n-chunk 4000 --no-plots
```

Hyperpipe supplies `--fname`, `--using-eos file:<grid>`,
`--using-eos-index`, and output names. `n-chunk` can request several EOS
hypotheses per worker; CIP reuses the fitted event likelihood and parsed
sequence. The EOS grid's parameters, including `sequence_index`, survive
in each `+annotation.dat` evidence table. Event products must match the
same EOS hypothesis across all events.

Run the same marginal job directly to inspect a chunk:

```bash
util_ConstructIntrinsicPosterior_GenericCoordinates.py \
  --fname event.net --using-eos file:ensemble.dat --using-eos-index 0 \
  --n-events-to-analyze 3 --chunk-save \
  --eos-param nmbseq --eos-sequence-file /shared/eos/sequence.h5 \
  --eos-affine-parameter central --eos-branch-prior mass-per-branch \
  --input-tides --parameter mc --parameter delta_mc \
  --parameter-implied LambdaTilde --fit-method quadratic \
  --mc-range '[1.55,1.70]' --eta-range '[0.2475,0.25]' \
  --source-redshift 0 --n-max 100000 --n-eff 1000 --n-chunk 4000 \
  --fname-output-integral MARG-0 --fname-output-samples samples-0 --no-plots
```

These commands assume shared storage, including the HDF5 file. For file-transfer
workers, add that file to the transfer list and use its worker-side path.
The local acceptance test does not establish OSG operation.

`sequence_index` is categorical. A finite ensemble can be evaluated and
weighted with explicit discrete prior probabilities. Do not fit or puff it
as a continuous EOS parameter. This example covers the marginalization
stage; continuous EOS generation requires a separate physical model.

For conditional equal branch occupation at each mass, request
`--eos-branch-prior uniform-at-mass`. This changes the measure. Neither
choice normalizes a mass prior over EOS-dependent support. Both retain the
fixed-redshift approximation. The per-process `_eos_branch.dat` sidecars
must stay beside their XML; generic XML joins do not merge branch labels.

The executable acceptance test constructs single-, twin- and three-branch
NMB sequences, assembles Hyperpipe marginal arguments, and runs real CIP
chunks. It compares event-integral ratios against direct branch-sum
quadrature and separate single-EOS calls:

```bash
PYTHONPATH=MonteCarloMarginalizeCode/Code \
  python -m pytest MonteCarloMarginalizeCode/Code/test/test_eos_multibranch_grid.py
```
