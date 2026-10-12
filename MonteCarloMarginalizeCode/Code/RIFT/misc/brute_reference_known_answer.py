"""Known-answer check of brute_reference_integral with t components: 6-D Gaussian peak
(sigma 0.01, unit box), exact lnI = 0 for L = N(x; c, s^2 I) / prior, p uniform on [0,1]^6."""
import numpy as np
from RIFT.misc.brute_reference import brute_reference_integral, _TruncT
rng = np.random.default_rng(3)
d, s, c = 6, 0.01, np.full(6, 0.37)
params = ["x%d" % i for i in range(d)]
def lnL(**kw):
    X = np.column_stack([kw[p] for p in params])
    return -0.5 * np.sum(((X - c) / s)**2, axis=1) - d * np.log(np.sqrt(2 * np.pi) * s)
pil = np.vstack([c + 3 * s * rng.standard_normal((20000, d)), rng.random((20000, d))])   # crude pilot
pw = np.exp(lnL(**{p: pil[:, i] for i, p in enumerate(params)}) - 20)
mu = np.average(pil, axis=0, weights=pw) + 0.5 * s; cv = np.cov(pil.T, aweights=pw)          # deliberately off-centre
lo, hi = np.zeros(d), np.ones(d)
ext = [_TruncT(mu, cv * 1.5**2, 5., lo, hi, rng), _TruncT(mu, cv * 16, 5., lo, hi, rng)]
out = brute_reference_integral(lnL, params, lo, hi, {p: (lambda x: np.ones_like(x)) for p in params}, pil,
                               n_total=400000, block=20000, nb_fine=10, nb_coarse=4,
                               weights=(0.25, 0.15, 0.10, 0.10), pilot_weights=pw,
                               extra_components=ext, extra_weights=[0.30, 0.10], verbose=False)
print("known answer 0: lnI %.4f +- %.4f  ESS %.0f  khat %.3f  (|lnI|/sigma %.2f)" % (
      out["lnI"], out["sigma_lnI"], out["ess"], out["khat"], abs(out["lnI"]) / out["sigma_lnI"]))
