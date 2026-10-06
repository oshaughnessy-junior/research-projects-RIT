# XPHM lossless-Q bootstrap profile

`blueprints/analysis_rift_XPHM_lossless_q.yaml` is an explicit analysis overlay,
not an event ledger or launch command. Inherit reviewed event data, channels,
PSDs, segment length, frequency bounds and physical priors. Replace the
bootstrap PESummary path and its exact analysis label. Pin an installed RIFT
revision containing this change, along with LALSuite and the runtime image;
older images cannot execute the new CIP option. Keep runtime/site settings in
the project ledger. Rendering and DAG generation do not authorize submission.

The profile seeds up to 10,000 physical posterior draws, disables reuse of an
unverified existing grid, and retains the last **two** CIP schedule rows: `Z`
(full-spin internal convergence subdag) and `1` (final posterior). Truncating to
one row would discard refinement. Both use fresh RF fits in the same lossless-Q
basis; sampling and the physical prior remain in the native coordinates.
Bootstrap density is a starting point, not evidence of convergence or coverage.
The usual puff/refinement behavior remains available to explore outside the seed.

At fixed masses and aligned spins, define `w1=(1+q)^-2`, `w2=q^2*w1`,
`L=eta/(pi*M*f_ref)^(1/3)` in geometrized units, `D=L+w1*s1z+w2*s2z`,
`T=w1*s1_perp+w2*s2_perp`, and `R=(-w2*s1_perp+w1*s2_perp)/hypot(w1,w2)`.
The four fit coordinates are `(Q, phi_T, R_parallel, R_perp)`, where
`Q=(hypot(D,|T|)-D)/L` and the residual components are resolved along/across T.
They retain four transverse degrees of freedom. On T=0 the azimuth is set to
zero; at negative D the Q domain starts at `-2D/L`. The polar angle wraps at
pi, so this chart is not globally smooth. No spin transport is performed.
The reference frequency must match the supplied physical spins.

The likelihood uses XPHM, 4096 Hz data, explicit cubic time interpolation, and
network sky coordinates. AV selects the maintained NoLoop ILE path; audit
`args_ile.txt` and final extrinsic arguments for all four required flags:
`--time-marginalization --vectorized --gpu --force-xpy`, plus
`--interpolate-time cubic --internal-sky-network-coordinates`.
The ordinary final extrinsic/time-resampling stage stays enabled; calibration
sampling/reweighting is disabled. Check the merged ledger has no inherited
in-loop calibration override, and that priors/frequency limits are unchanged.

Before any separately approved run, inspect the rendered INI, both CIP rows and
the generated internal subdag, pin bootstrap provenance, and confirm the
worker imports the pinned implementation. Compare native and Q fits with the
same evaluated likelihoods and seeds before attributing posterior differences
to coordinates. Existing-grid validation alone cannot establish full-rerun
recovery with the new time/sky settings.
