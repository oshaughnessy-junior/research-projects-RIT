# Part 0 -- NoLoop MC-layer gate (closes the T4 MC caveat)

`run_ile_mc_noloop.sh` reaches the robust production likelihood
`DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop` via `--gpu --vectorized --force-xpy`.
Without a GPU, `xpy` falls back to numpy (expected + fine) and the NoLoop path uses the
pure-numpy `SphericalHarmonicsVectorized`, NOT the numba `lalylm` that blocked the plain-CPU
`DiscreteFactoredLogLikelihoodViaArrayVector` in T4.  So the real MC `lnL_marg` is produced on
both fd-diagonal and td-toeplitz.

## Result (seed 1234, n-max 40000, srate 2048, 8 s, H1+L1, BBH 35+35, zero noise)

Which likelihood ran: the logs show `Override --gpu (not available); use --force-xpy ...`
followed by ` Using CUDA GPU likelihood, if cupy available ` -- i.e. `opts.gpu` is forced true
so ILE takes the xpy branch calling `DiscreteFactoredLogLikelihoodViaArrayVectorNoLoop`
(`bin/integrate_likelihood_extrinsic_batchmode:1744`).  The td run additionally prints the
`[TD] Using time-domain Toeplitz inner product` banner, confirming the Toeplitz precision
kernel is active in Precompute.

| run | lnL_marg | MC error (sigma/L, ILE col 10) | neff |
|---|---|---|---|
| fd-diagonal (inv-spec-trunc 0) | 368.81996659911783 | 0.42766 | 4.49 |
| td-toeplitz                    | 368.81996659905201 | 0.42766 | 4.49 |

**Per-point `|Delta lnL_marg| = 6.6e-11`** -- 9 orders of magnitude inside the 0.05 gate, and
far below the ILE-reported MC error (`sigma/L = 0.428`).  The fd/td difference is common-mode
(identical seed/sampler; the two runs differ only in the Precompute inner product, which
agrees to ~1e-13), so the differential is set by Precompute parity, not by the MC noise.

Note: the low `neff` (4.49) is a GMM-sampler-quality artifact of this deliberately cheap
smooth-PSD demo (the GMM occasionally fails to train and resets, then recovers) -- it hits fd
and td identically and does not affect the differential gate.  The MC-layer caveat from T4 is
therefore closed: the NoLoop path produces a real `lnL_marg` and fd == td to 6.6e-11.

Reproduce: `./run_ile_mc_noloop.sh 40000 3000`.
