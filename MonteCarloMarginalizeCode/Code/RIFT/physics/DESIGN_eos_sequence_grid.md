# Stable-branch EOS evidence on a finite sequence grid

This record describes the EOS grid contract as of 2026-11-09. It may be
superseded. The tested implementation defines the current interface.

CIP accepts an NMB sequence as a collection of fixed EOS hypotheses. The
numeric Hyperpipe grid keeps its leading `lnL sigma_lnL` columns. Exactly
one later column, `sequence_index`, selects a native HDF5 sequence row.
Other columns are retained in the marginal evidence table. The file is
parsed once per worker with sorting disabled, so index identity survives
all grid rows and chunks. Invalid indices reject their proposed EOS row.
Missing files, malformed headers and incompatible producer formats are
configuration errors.

`--eos-affine-parameter central` sums only stable branches. The default
`--eos-branch-prior mass-per-branch` assigns the mass measure to every
stable branch. At fixed masses, uniform auxiliary labels choose branches
with probability `1/n(m)` and the integrand receives `n(m)` per star.
This is equivalent to an affine integral with `|dm/ds|` on each stable
segment. It does not assume a prior uniform in central enthalpy.

The optional `uniform-at-mass` convention assigns each available branch
conditional probability `1/n(m)`. It removes the multiplicity factor.
Neither convention renormalizes the source-mass prior over EOS-dependent
stable support. Such a normalization belongs to the declared population
model; changing it changes the evidence.

`sequence_index` is categorical. Evaluating a finite grid and combining its
per-event likelihoods is supported. Fitting or puffing a continuous index
is not an EOS-generation model. Continuous hyperparameter proposals need
a constructor or coordinate plugin that supplies a corresponding EOS.

The fixed-redshift conversion remains in use. Distance-conditioned EOS
likelihoods require distance and redshift in the likelihood model; these
options do not add that marginalization.

CIP exports each sampled branch identity beside its XML samples. Pipeline
XML joins do not propagate that sidecar. Event evidence tables retain the
EOS index, which is sufficient for the finite-grid product over events.
Raw posterior branch labels must be kept with each worker's XML.

The example in `demo/eos_multibranch/README.md` assumes shared storage.
Nonshared workers require explicit HDF5 transfer and a worker-visible path.
No OSG acceptance claim is attached to the local grid-driver test.
