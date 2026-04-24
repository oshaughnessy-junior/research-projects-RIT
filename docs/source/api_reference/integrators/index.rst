Integrators (MC Sampling)
=========================

Monte Carlo integration modules for likelihood evaluation and parameter inference.

Overview
--------

These modules provide various strategies for Monte Carlo integration, used throughout RIFT
for parameter inference and evidence computation. The integrators work in conjunction with
:doc:`likelihood modules <../likelihood/index>` to evaluate gravitational wave signals
against data.

.. seealso::

   - :doc:`../likelihood/index` - Likelihood evaluation modules
   - :doc:`../interpolators/index` - Surrogate models for fast likelihood evaluation

Base Integrators
----------------

.. automodule:: RIFT.integrators.mcsampler_generic
   :members:
   :undoc-members:
   :show-inheritance:

Standard MC Sampler
-------------------

.. automodule:: RIFT.integrators.mcsampler
   :members:
   :undoc-members:
   :show-inheritance:

MC Sampler with Adaptive Volume
-------------------------------

.. automodule:: RIFT.integrators.mcsamplerAdaptiveVolume
   :members:
   :undoc-members:
   :show-inheritance:

Ensemble MC Sampler
-------------------

.. automodule:: RIFT.integrators.mcsamplerEnsemble
   :members:
   :undoc-members:
   :show-inheritance:

GPU MC Sampler
--------------

.. automodule:: RIFT.integrators.mcsamplerGPU
   :members:
   :undoc-members:
   :show-inheritance:

Portfolio MC Sampler
--------------------

.. automodule:: RIFT.integrators.mcsamplerPortfolio
   :members:
   :undoc-members:
   :show-inheritance:

Normalizing Flows MC Sampler
----------------------------

.. automodule:: RIFT.integrators.mcsamplerNFlow
   :members:
   :undoc-members:
   :show-inheritance:

Monte Carlo Ensemble
--------------------

.. automodule:: RIFT.integrators.MonteCarloEnsemble
   :members:
   :undoc-members:
   :show-inheritance:

Weighted GMM
------------

.. automodule:: RIFT.integrators.weighted_gmm
   :members:
   :undoc-members:
   :show-inheritance:

Statistical Utilities
---------------------

.. automodule:: RIFT.integrators.statutils
   :members:
   :undoc-members:
   :show-inheritance:

Gaussian Mixture Model
----------------------

.. automodule:: RIFT.integrators.gaussian_mixture_model
   :members:
   :undoc-members:
   :show-inheritance:

Direct Quadrature
-----------------

.. automodule:: RIFT.integrators.direct_quadrature
   :members:
   :undoc-members:
   :show-inheritance:

Multivariate Truncated Normal
-----------------------------

.. automodule:: RIFT.integrators.multivariate_truncnorm
   :members:
   :undoc-members:
   :show-inheritance:

Unreliable Oracle
-----------------

.. automodule:: RIFT.integrators.unreliable_oracle
   :members:
   :undoc-members:
   :show-inheritance: