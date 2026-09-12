Mock population studies
=======================

RIFT includes a demonstration workflow for turning a mock compact-binary
population into individual RIFT parameter-estimation (PE) runs.  The workflow
lives in ``MonteCarloMarginalizeCode/Code/demo/populations`` and is intended
to connect mock injections made with `GWKokab <https://gwkokab.readthedocs.io/>`_
to RIFT PEs.

This is a site-oriented demonstration, not a general-purpose population
inference interface.  It assumes a separate GWKokab environment, an RIFT
environment based on an IGWN installation, and access to LDG/HTCondor-style
computing resources.  In particular, ``make submit`` submits a production
DAG and can run for days; do not use it as a local quickstart.

Prepare the two environments
----------------------------

Keep GWKokab and RIFT in separate environments to avoid dependency conflicts.
First generate a mock realization with GWKokab.  Its output should include an
``injections.dat`` file, with one compact-binary system per row.  Before
launching RIFT PEs, validate the mock population with the relevant GWKokab
inference example.

Then configure the population demo's ``Makefile`` for the local installation.
At a minimum, set the RIFT environment name, repository locations, and the
absolute paths for the run directory, input population, and analysis ini file.
The Makefile's ``setup-env`` target is specifically written for an
LDG/``igwn-py310`` environment; review it before running it at another site.

Match injections and PE settings
--------------------------------

Copy the generated ``injections.dat`` into the demo directory and add a
luminosity-distance column before building injections for RIFT:

.. code-block:: console

   python lum_distance.py --input injections.dat --output injections.dat

Set all analysis choices in ``pop-example.ini``.  The prior ranges and model
choices in that file must match the population used to generate the injections.
Inconsistent mass, spin, eccentricity, or redshift ranges make the resulting
PEs unsuitable for the intended population study.  Also update site-specific
noise-frame paths and accounting/user settings before creating run directories.

Create and inspect the workflow
-------------------------------

The demo Makefile provides the following staged commands:

.. code-block:: console

   make injections
   make rundir

``make injections`` invokes ``write_mdc.py`` to convert the population and
write the RIFT MDC material under the configured run directory.  ``make
rundir`` generates frames and invokes ``pp_RIFT_with_ini`` to create one RIFT
run directory per event.  Ensure the required PSD files are available in each
run directory before submitting any work.

After inspecting the generated directories and submit files, a suitably
authorized LDG/Condor user can submit the PE workflow:

.. code-block:: console

   make submit

Monitor and diagnose these jobs with the local scheduler tooling.  The demo's
``collect_all.sh`` script collects completed
``extrinsic_posterior_samples.dat`` files; ``gwk_pop_conversion.py`` can then
prepare those samples for the downstream GWKokab workflow.  Check that each
event produced a completed posterior before treating its samples as input to
population inference.
