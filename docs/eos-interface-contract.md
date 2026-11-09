# EOS interface contract across RIFT, LALSimulation, and NuclearMatter-Backend

RIFT keeps the historical fixed-EOS consumer surface:

```python
eos.lambda_from_m(m)
eos.lambda_from_m_vector(masses)
eos.mMaxMsun
```

Mass arguments may be in solar masses or SI kg, as before.

Multibranch EOS objects add:

```python
eos.branches_for_m(m)                # stable branch ids containing m
eos.lambda_from_m(m, branch_id=b)
eos.lambda_from_m_vector(m, branch_id=b)
eos.for_branch(b)                    # scalar view pinned to one branch
eos.mMaxMsun_all                     # NMB only; lalsim mMaxMsun is already all-branch
```

## Sample evaluation failure policy

Waveform and EOS evaluation failures reject the affected sample and continue
the analysis. This is a design principle for both waveform and EOS sampling.

Exceptions from the waveform or EOS implementation, and nonfinite results,
must follow the existing invalid-sample convention: zero posterior weight or
`-inf` log likelihood. Sampling boundaries translate these failures into sample
rejection. They must not terminate the sampler, worker, batch, or workflow.
Batched evaluation must preserve valid neighboring samples when one evaluation
fails.

A rejected proposed EOS emits log evidence `-inf` and relative error `0`.
A proposed EOS whose actual conversions reject every draw records zero evidence
after the sampler exhausts its existing retry budget. The first invalid batch
does not end those retries; later valid draws remain usable. Unrelated sampler
errors retain their existing behavior.

Repeated estimates combined by an average or sum retain successful replicas;
all failed replicas retain zero evidence. A product over events retains every
event, so a zero-evidence event makes the product zero.

Compatibility changes must preserve this behavior. Regression tests must mix
valid and failing samples, exercise both raised exceptions and nonfinite
outputs, and verify that failed samples are rejected while valid samples remain
usable and sampling continues.

## LALSimulation families

Legacy LALSimulation exposes one stable family. Released multibranch builds
and the reviewed multipart TOV API can return several stable branches.  RIFT exposes
`branches_for_m(m)` and accepts `branch_id` on direct `radius_from_m` and
`lambda_from_m` calls.  An ambiguous twin-star mass raises instead of silently
choosing a solution.

Legacy scalar consumers select a branch once with `eos.for_branch(branch_id)`.
The fixed-EOS CIP driver exposes the same operation as:

```text
--using-eos lal_<name> --using-eos-branch <integer>
```

A selected branch is bounded at both ends, so the view publishes `mMinMsun`
alongside `mMaxMsun`; a twin-star branch can begin well above the family
minimum mass. Consumers that mask draws should mask against both bounds.
Because the historical consumers mask only on `mMaxMsun`, `lambda_from_m` (and
therefore `lambda_from_m_vector`) returns `-inf` for a mass with no stable star
on the selected branch rather than raising, so a single out-of-branch draw
flags itself instead of aborting the batch it arrived in.

The flag is only useful if the consumer acts on it, and the consumer cannot
read it off the converted coordinates.
`lalsimutils.convert_waveform_coordinates_with_eos` returns only `coord_names`,
so with a mass-only fit basis such as `mc,eta` the lambda carrying the flag is
computed and then discarded, and the row reaches the fit fully finite. On the
vectorized path an above-`mMaxMsun` mass is worse than invisible: it arrives as
`lambda = 0`, which reads as a black hole rather than as "no such star".

The support test is therefore made against the sampled MASSES, by
`RIFT.physics.lalsim_eos_compat.mass_in_eos_support`, which tests each object
against whichever of `mMinMsun` and `mMaxMsun` the EOS publishes and exempts any
object declared a black hole by `--no-matter1` / `--no-matter2` (or
`--assume-eos-but-primary-bh` on export). A bound the EOS does not publish is
read as unbounded, which leaves EOS classes exposing neither unchanged. With a
fixed EOS, CIP applies that test unconditionally -- not only under
`--protect-coordinate-conversions` -- stamping out-of-support rows `-inf` before
the likelihood fit, so they carry zero probability while the rest of the batch
is fit normally in one call. The export path applies the SAME test before asking
the EOS for lambda at all, so the fit and the export cannot disagree about which
draws exist.

Reviewed two- or nine-column tables use:

```text
--using-eos lalsim_file:<path> [--using-eos-dirty-phase-transitions]
    [--using-eos-extended-family] [--using-eos-branch <integer>]
```

By default, `lalsim_file:` preserves released
`SimNeutronStarEOSFromFile` behavior even on a reviewed build. Requesting a
branch, the compatibility dirty-phase-transition option, or the extended
family opts into `SimNeutronStarEOSFromFilePhaseTransition` followed by
`CreateSimNeutronStarFamilyPT(eos, min_fam)`. The historical
`--using-eos-dirty-phase-transitions` spelling is retained as a compatibility
request, but the reviewed SWIG interface has one phase-transition reader; it
does not expose a separate dirty-table boolean. On released LALSimulation,
that flag fails explicitly rather than pretending the legacy reader repaired a
phase transition.

The Python API also accepts `family_log_pressure_min=<value>` on
`EOSLALSimulationFromFile`. This opts into
`CreateSimNeutronStarFamilyPTWithPcmin(eos, min_fam, logPcmin)`; the value must
be finite and is the natural logarithm `ln(Pc / Pa)`. Mass-radius helpers use
the canonical `multipart=` keyword and retain `reviewed_multibranch=` as a
deprecated O4c compatibility alias; conflicting explicit values raise.

Branch-specific causality checks dispatch to the multipart accessors
`SimNeutronStarEOSMultiPartsPseudoEnthalpyOfPressure` and
`SimNeutronStarEOSMultiPartsSpeedOfSoundOfPseudoEnthalpy`; they never pass a
`SimEOSMultiParts` object to a legacy single-EOS accessor. The multipart
sound-speed accessor returns SI metres per second, so RIFT
divides by `lal.C_SI` before applying its historical dimensionless `v/c < 1.1`
test. Full-table causality uses
`SimNeutronStarEOSMultiPartsMaxPseudoEnthalpy`; it fails closed if that reviewed
symbol is unavailable.

For pseudo-pipe workflows, forward the flag with
`--manual-extra-cip-args`.  On O4d, Hydra hyperpipe configurations can put it
in the post driver's `extra-args` when that driver is the fixed-EOS CIP
executable.

## NuclearMatter-Backend sequences

The `nmbackend.nss/1` (`tabular_hc/1`) and `nmbackend.pca/1` (`pca_hc/1`)
producer tags, field names, and `EOSSequenceFromFile` dispatch remain unchanged.
The nmb-papers exact-evidence command remains:

```text
--using-eos nmbseq:<sequence.h5>:<index>
```

By default this command projects each EOS onto its **primary stable
mass-rising branch**: `lambda_from_m(m)` without a branch id and `mMaxMsun`
both refer to the primary branch.  RIFT splits central-enthalpy-ordered data at
unstable or decreasing-mass intervals before M-to-Lambda interpolation, so it
never mixes disconnected branches.  `stable_branch_counts[index]` records when a
native sequence contains more than one stable run.

`nmbseq:` objects now carry the branch surface above, with `mMaxMsun_all` as the
maximum over every branch.  Two choices are available beyond the primary
branch:

- `--using-eos-branch <k>` analyzes one chosen branch, as for LALSimulation.
- `--eos-affine-parameter central` includes every stable branch (next section).

Unlike LALSimulation, an NMB sequence returns the primary branch for an
ambiguous twin mass when no branch id is given; LALSimulation raises.

## Including every stable branch: `--eos-affine-parameter central`

```text
--using-eos <spec> --eos-affine-parameter central
```

The default, `--eos-affine-parameter mass`, is the historical one-branch
`M -> Lambda` map and runs the same code as before.  `central` uses position
along the stellar sequence (central density or enthalpy) as the affine
parameter, so every stable branch contributes.  Each component may be a star on
any stable branch.

The prior is dM per stable branch.  A mass with n(m) stable branches counts
n(m) times, so the joint mass prior gains a factor n(m1) n(m2).  For a
single-branch EOS, n = 1 and the prior is the usual mass prior.  Twin regions
are overcounted by design.

CIP keeps its sampling coordinates and the trained likelihood fit.  It adds two
sampled variates, `eos_branch_u1` and `eos_branch_u2`, uniform on [0, 1).  They
are not fit coordinates.  For component i:

| Step | Definition |
|---|---|
| branches | B_i = `eos.branches_for_m(m_i)`, n_i = len(B_i) |
| branch | b_i = B_i[floor(u_i n_i)] |
| tide | Lambda_i = `eos.lambda_from_m(m_i, branch_id=b_i)` |
| weight | integrand times n_1 n_2 |

Integrating over u_i then sums the integrand over branches with the
dM-per-branch prior.  The factor n_1 n_2 enters the integrand beside the
existing prior-scale correction.  It therefore multiplies the evidence and the
posterior weights for every sampler.  `adaptive_cartesian`, `GMM` (with or
without `--internal-use-lnL`) and `AV` have been checked against exact branch
sums.  With `GMM`, each variate gets its own one-dimensional mixture group.

A mass on no stable branch, or at or above the all-branch maximum
`getattr(eos, 'mMaxMsun_all', eos.mMaxMsun)`, rejects the sample.  The mass path
applies the same rule at `mMaxMsun`.  Masses below the lowest stable branch are
also rejected in central mode.  `--no-matter1` and `--no-matter2` give that
component Lambda = 0 and weight 1.

Exported samples keep the `sim_inspiral` XML format, with Lambda from the
sampled branch.  The XML has no branch column, so CIP writes
`<fname-output-samples>_eos_branch.dat` with columns `b1 b2 n1 n2`, row for row
with the XML.  The ids follow component swaps; `-1` marks a component without
matter.

Central mode fails at startup with a clear error for these combinations:

- no `--using-eos`, or an EOS without `branches_for_m` or without a `branch_id`
  keyword on `lambda_from_m`;
- `--using-eos-branch`, which selects one branch;
- `--tabular-eos-file`: the fit over the ordering coordinate cannot represent
  branch labels;
- `--using-eos-for-prior`: the hyperprior plugin contract carries no branch
  identity;
- `--assume-eos-but-primary-bh` without `--no-matter1`;
- `--sampler-oracle` or `--oracle-reference-sample-file`: reference samples have
  no branch variates.

A supplementary likelihood factor receives the native sampling coordinates
only, as in mass mode.  The sample evaluation failure policy above applies
unchanged.  Exceptions or nonfinite values from `branches_for_m` or
`lambda_from_m` reject that sample and keep its neighbors.

`util_RIFT_pseudo_pipe.py` has no fixed-EOS CIP options, so forward
`--using-eos ... --eos-affine-parameter central` with `--manual-extra-cip-args`.

## Downstream compatibility

- Existing Kedia-style spectral and piecewise-polytrope EOS inference keeps the
  same constructor and scalar lookup APIs.  Ordinary single-branch models need
  no new option.
- Existing nmb-papers and hyperpipe fixed-sequence runs keep the same HDF5 tags
  and `nmbseq:` syntax.
- Twin-star analyses select one branch with `--using-eos-branch`, or include
  every branch with `--eos-affine-parameter central`.

`--using-eos-branch` and `--eos-affine-parameter central` are rejected with
`--using-eos-for-prior`. The current EOS-hyperprior plugin protocol returns an
EOS but does not return branch identity, so accepting either combination would
analyze the wrong branch without warning. A future multibranch hyperprior must extend that plugin contract before
this restriction can be relaxed.

## Reviewed-LALSimulation integration gate

The fake-backed compatibility tests check RIFT's dispatch logic, but do not
certify the reviewed SWIG interface. Fixture-free acceptance uses trusted
`SimNeutronStarEOSMultiPartsByName("SLY")`, checks coexistence with the legacy
named-EOS API, and repeatedly constructs and destroys families. To run it,
build the exact reviewed LALSuite commit, activate that Python environment, and set
`RIFT_REVIEWED_LALSIM_MANIFEST` to a JSON file with this shape:

```json
{
  "lalsuite_ref": "974c0ef468b76e8298e67fd8baf71ed259cc5fee",
  "fixtures": {
    "two_column": {"path": "two-column.dat", "sha256": "..."},
    "nine_column": {"path": "nine-column.dat", "sha256": "..."},
    "twin_star": {"path": "twin-star.dat", "sha256": "...", "columns": 9}
  },
  "known_upstream_crash": {
    "path": "eos_2pt.dat",
    "sha256": "..."
  }
}
```

The `fixtures` object is optional, and `twin_star` is optional within it.
Without fixtures, builtin acceptance runs and external-table coverage is an
explicit skip. File-loader fixtures must be explicit two- or nine-column
numeric tables. Four-column wiki arrays require a separate, provenance-recorded
unit conversion and are not passed to the native file reader. Each external
table is loaded in a subprocess with a 60-second timeout, output sent to
`DEVNULL`, and only a compact JSON status file returned to pytest.

The optional `known_upstream_crash` entry is converted through
`SimNeutronStarEOSFromArraysPhaseTransition` using the reviewed geometrized
factor `1.602176634e32 * G / c^4`. The authoritative two-transition
four-column array fixture currently exits
with signal 11 (shell return code 139) inside the reviewed native implementation.
That is an upstream acceptance blocker, not a passing RIFT fixture. It must be
rerun through a separately provenance-recorded array conversion after the
native crash is fixed; this gate does not silently reinterpret it as a file
reader input.

Run
`pytest MonteCarloMarginalizeCode/Code/test/test_lalsim_eos_reviewed_integration.py`.
Paths are relative to the manifest. The ref must be the full 40-character
commit actually built by the job: the gate requires it to equal
`lalsimulation.SimulationVCSInfo.vcsId` and requires a clean VCS build. Every
supplied fixture hash is mandatory. Once the manifest enables the gate, missing modern
symbols, malformed or mismatched build provenance, missing fixtures, wrong
column counts, or absence of distinct overlapping twin-star solutions are
failures. The gate exercises the real phase-transition reader on two- and
nine-column inputs, minimal and extended family construction, and
branch-indexed radius, Love
number, central pressure, and tidal deformability. Ordinary CI skips this
private-build gate explicitly.

The drift-sentinel registry should eventually declare an EOS contract group
with LALSuite and NuclearMatter-Backend as producers and RIFT/nmb-papers as
consumers.  The current registry only covers RIFT/hyperpipe operational archive
and queue boundaries, so it cannot detect EOS schema or callable drift yet.
