=========
HyperPipe
=========

HyperPipe is RIFT's generalized, coordinate-free parameter-estimation pipeline.
Use it when you have an executable that can evaluate a likelihood for rows in a
parameter grid and you want RIFT to manage the adaptive loop: evaluate the grid,
combine the likelihoods, draw a posterior, place the next grid, and repeat.

The key difference from gravitational-wave-specific RIFT workflows is that
HyperPipe does not know or care what the coordinates mean.  The user supplies
parameter names, coordinate ranges, and one or more likelihood drivers.  The
pipeline supplies the repeatable HTCondor workflow around those drivers.

When should you use HyperPipe?
==============================

Use HyperPipe when:

* your scientific problem can be written as ``lnL(parameters)``;
* likelihood evaluations are expensive enough to parallelize over many jobs;
* you need adaptive placement rather than a single fixed grid; and
* you want the output in RIFT-compatible posterior/grid formats.

For a first run, start from the repository demo rather than from a blank
configuration file:

``$RIFT_ROOT/MonteCarloMarginalizeCode/Code/demo/hyperpipe``

That directory contains toy Gaussian drivers, YAML configurations, and a
``Makefile`` that creates runnable demonstration directories.

Environment Setup
=================

HyperPipe requires a configured RIFT environment. If you are using a standard
cluster installation, activate your environment before running any commands:

.. code-block:: console

   $ conda activate rift-env

Ensure that the HyperPipe executables (e.g., ``util_RIFT_hyperpipe.py``) are
available on your ``PATH``. If they are not, add the RIFT bin directory to your
profile or use absolute paths.

The adaptive loop
=================

HyperPipe repeats the same conceptual loop for each iteration:

.. code-block:: text

   grid-k.dat
       |
       v
   MARG jobs: user likelihood driver evaluates lnL on chunks of the grid
       |
       v
   CON / CON_PROD: per-job and per-constraint outputs are consolidated
       |
       v
   UNIFY: all evaluated likelihood points are accumulated into all.marg_net
       |
       v
   EOS_POST: likelihood x prior is sampled to draw posterior points
       |
       v
   PUFF or tracer: next exploration grid is widened or replaced
       |
       v
   grid-(k+1).dat

The ``MARG`` jobs are the high-volume worker jobs.  They run the user's physics
or simulation executable over slices of the grid.  The ``EOS_POST`` and
placement stages are lower-volume fitting/posterior jobs that decide where the
next iteration should look.

Quick start: run the Gaussian demo
==================================

From a RIFT environment with the HyperPipe executables on ``PATH``:

.. code-block:: console

   $ cd MonteCarloMarginalizeCode/Code/demo/hyperpipe
   $ make rundir
   $ cd rundir
   $ condor_submit_dag marginalize_hyperparameters.dag
   $ watch condor_q

A successful first iteration should leave evaluated likelihood tables and new
placement/posterior products in the run directory.  Useful first checks are:

.. code-block:: console

   $ ls grid-*.dat all.marg_net consolidated_*.net_marg
   $ find iteration_0_* -maxdepth 2 -type f | head
   $ tail -n 40 *.dag.nodes.log

If ``all.marg_net`` is absent or empty, debug the ``MARG`` and consolidation
stages before changing posterior settings.

The baseline demo uses ``hyperpipe_conf.yaml`` and the toy driver
``example_gaussian.py``.  A tracer-based variant is available as:

.. code-block:: console

   $ cd MonteCarloMarginalizeCode/Code/demo/hyperpipe
   $ make rundir_tracer

or directly:

.. code-block:: console

   $ util_RIFT_hyperpipe.py --config ./hyperpipe_conf_tracer.yaml

Use the demo ``Makefile`` as a template for your own paths, driver arguments,
parameter ranges, and output directory names.

Configuration model
===================

Modern HyperPipe runs are configured with a Hydra/OmegaConf YAML file consumed
by ``util_RIFT_hyperpipe.py``.  The default schema lives in
``RIFT.hyperpipe.config`` and has these top-level sections:

.. code-block:: yaml

   arch:
     method: default
     n-iterations: 5
     n-samples-per-job: 1000
     explode-marg-jobs: 5

   post:
     coords-fit: "x y z"
     coords-sample: "x:[-7,7] y:[-7,7] z:[-7,7]"

   marg-list:
     - name: Gaussian
       exe: example_gaussian.py
       args: "--outdir Gaussian_example --conforming-output-name"
       n-chunk: 100

   puff:
     puff-factor: 0.5
     force-away: 0.03

   test:
     exe: convergence_test_samples.py
     method: JS
     threshold: 0.05
     extra-args: ""

   init:
     file: blind_gaussian_3d_xy_plus.dat

   general:
     rundir: rundir
     request-memory: 200

``arch``
   Iteration count, chunking, and high-level workflow controls.

``post``
   Posterior-construction settings.  ``coords-fit`` lists parameters included
   in the fit; ``coords-sample`` gives integration ranges.  Optional fields
   include ``coord-module``, ``coords-implied``, ``coords-nofit``, and
   ``extra-args``.

``marg-list``
   One entry per likelihood constraint or event.  Each entry names the driver
   executable, its arguments, and its chunking.  Multiple entries let HyperPipe
   evaluate several constraints in the same adaptive loop.

``puff``
   Placement refinement.  With no ``exe`` set, HyperPipe resolves the legacy
   puffball executable and emits the standard puff placement path.  A
   tracer-aware updater can be configured here to read ``all.marg_net``
   directly and avoid the legacy ``MARG_PUFF`` lane.

``test``
   Convergence-test settings.  ``method`` selects the divergence metric,
   ``threshold`` controls when the DAG can halt, and ``extra-args`` is appended
   to the test executable.  The default executable is
   ``convergence_test_samples.py``.

``init``
   The initial grid.  Provide ``file`` for an existing grid, or a
   ``generation`` block to create one from parameter ranges.

``general``
   Run-directory, resource, Condor, OSG, Singularity, and file-transfer knobs.

Creating a run directory
========================

``util_RIFT_hyperpipe.py`` translates the YAML configuration into the legacy
``create_eos_posterior_pipeline`` interface, writes args files, creates Condor
``*.sub`` files, and constructs ``marginalize_hyperparameters.dag``.

For debugging or legacy workflows, the underlying command looks like:

.. code-block:: console

   $ create_eos_posterior_pipeline \
       --marg-event-exe-list-file `pwd`/args_marg_eos_exe.txt \
       --marg-event-args-list-file `pwd`/args_marg_eos.txt \
       --eos-post-args `pwd`/args_eos_post.txt \
       --eos-post-exe `which util_ConstructEOSPosterior.py` \
       --puff-exe `which util_HyperparameterPuffball.py` \
       --puff-args `pwd`/args_puff.txt \
       --input-grid initial_grid.dat \
       --n-samples-per-job 1000 \
       --use-full-submit-paths \
       --working-dir `pwd` \
       --event-file `pwd`/my_event_A.txt \
       --n-iterations 5 \
       --eos-post-explode-jobs 5

Run ``create_eos_posterior_pipeline --help`` before hand-editing this layer;
most users should edit the YAML and regenerate the run directory instead.

Input files and driver contract
===============================

Initial grid
------------

The grid is a whitespace-delimited text file.  The first row is a header and
must start with the likelihood columns followed by parameter names:

.. code-block:: text

   # lnL sigma_lnL x y z
   0   0           -5.0 2.0 2.0
   0   0           -4.9 2.1 2.0

The first two columns are overwritten by the likelihood driver as the pipeline
runs.  If your executable wants a different input format, write a wrapper that
translates from the RIFT grid row to your code's native format; do not make the
pipeline consume a private format.

A simple grid can be generated with ``util_HyperparameterGrid.py``:

.. code-block:: console

   $ util_HyperparameterGrid.py \
       --random-parameter x --random-parameter-range '[-5,-2]' \
       --random-parameter y --random-parameter-range '[2,5]' \
       --random-parameter z --random-parameter-range '[2,5]' \
       --npts 1000 --fname-out gaussian.dat

Likelihood driver
-----------------

A driver must read a grid slice, evaluate ``lnL`` for each row, and write a
RIFT-format annotated output file.  Drivers based on ``MargDriverBase`` get the
standard CLI and file I/O for free; subclasses mainly implement
``log_likelihood``.

The adaptive pipeline normally calls drivers in index mode.  A production
driver should accept these pipeline-facing flags:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Flag
     - Purpose
   * - ``--using-eos``
     - Input grid path.  A ``file:`` prefix is tolerated by ``MargDriverBase``.
   * - ``--using-eos-index``
     - First grid row assigned to this worker.  The generated DAG passes this
       from the Condor event macro.
   * - ``--n-events-to-analyze``
     - Number of grid rows assigned to this worker; usually the entry's
       ``n-chunk``.
   * - ``--outdir``
     - Output directory for driver products.
   * - ``--fname-output-integral``
     - Base name for the likelihood output file.
   * - ``--fname-output-samples``
     - Passthrough sample-output name expected by the pipeline interface.
   * - ``--conforming-output-name``
     - Append ``+annotation.dat`` so consolidation scripts can find the output.
   * - ``--fname``
     - Legacy passthrough expected by older RIFT invocation paths.

``MargDriverBase`` also accepts explicit range-mode helpers,
``--eos_start_index`` and ``--eos_end_index``.  These are useful for hand tests
and legacy wrappers, but they are not the usual Condor path for generated
HyperPipe DAGs.

A minimal custom driver looks like:

.. code-block:: python

   from RIFT.hyperpipe.drivers.base import MargDriverBase

   class MyDriver(MargDriverBase):
       description = "Toy quadratic likelihood."

       def log_likelihood(self, row_values, column_names, opts):
           params = dict(zip(column_names, map(float, row_values)))
           lnL = -0.5 * (params["x"]**2 + params["y"]**2)
           return lnL, 1e-3

   if __name__ == "__main__":
       MyDriver().run()

Driver development lifecycle
----------------------------

1. Start from ``example_gaussian.py`` or subclass ``MargDriverBase``.
2. Run the driver on a tiny grid by hand and confirm it writes a header plus
   ``lnL sigma_lnL`` columns.
3. Add the driver to ``marg-list`` with ``--conforming-output-name`` in
   ``args``.
4. Generate a run directory with ``util_RIFT_hyperpipe.py --config`` and inspect
   ``args_marg_eos*.txt`` before submitting to Condor.
5. Submit the DAG only after the generated args point at the expected driver,
   event file, grid, and output directory.

Run directory and outputs
=========================

A generated run directory has the following high-level structure.  Exact submit
files depend on placement mode, constraints, and convergence settings; treat the
``*.sub`` list below as representative rather than guaranteed for every config.

.. code-block:: text

   rundir/
      grid-0.dat                         # initial seed grid
      local.cache                        # Condor file cache
      iteration_0_marg/                  # MARG worker outputs and logs
      iteration_0_post/                  # posterior-construction outputs
      iteration_0_con/                   # consolidation outputs
      iteration_1_marg/
      ...
      MARG_0.sub                         # Condor submit file for first MARG lane
      CON.sub
      CON_PROD.sub
      UNIFY.sub
      EOS_POST.sub
      JOIN_POST.sub
      PUFF.sub
      marginalize_hyperparameters.dag    # top-level DAG

Important products:

``grid-0.dat``
   Initial grid.  You may supply your own grid or generate one from the config.

``grid-*.dat``
   Posterior-derived grids for later iterations.  These are compatible with
   downstream RIFT postprocessing tools.

``iteration_*``
   Per-iteration worker, consolidation, posterior, and log directories.

``consolidated_*.net_marg``
   Per-iteration consolidated marginalized-likelihood tables.

``all.marg_net``
   Cumulative likelihood table across iterations.  This is the main forensic
   record of what likelihood points have actually been evaluated.

``marginalize_hyperparameters.dag``
   The DAG submitted to HTCondor.

Pipeline stages
===============

``MARG.sub``
   Calls the user-provided likelihood driver over grid chunks.  If many jobs
   fail here, first inspect the driver CLI contract and input grid header.

``MARG_PUFF.sub``
   Legacy lane for evaluating puffed-grid points.  Tracer configurations can
   suppress this lane by consuming ``all.marg_net`` directly.

``CON.sub`` and ``CON_PROD.sub``
   Consolidate per-job outputs.  ``CON`` joins chunks for a single constraint;
   ``CON_PROD`` combines constraints into one overall result.  The default
   combination is product/multiplication of likelihoods.

``UNIFY.sub``
   Accumulates evaluated points from current and previous iterations into the
   cumulative ``all.marg_net`` table.

``EOS_POST.sub``
   Runs ``util_ConstructEOSPosterior.py`` to perform Monte Carlo integration of
   likelihood times prior and draw posterior samples for the next grid.

``PUFF.sub``
   Widens or refines placement around posterior-supported regions.  In legacy
   mode this uses ``util_HyperparameterPuffball.py``; in tracer mode this can be
   a tracer-aware updater such as ``util_HyperparameterTracerUpdate.py``.

``TEST.sub``
   Runs the configured convergence test and can stop the DAG when the selected
   divergence metric falls below ``test.threshold``.  Tune ``test.method`` and
   ``test.extra-args`` in the YAML when diagnostic runs should continue longer
   or use a different divergence measure.

Advanced features
=================

Multiple constraints
--------------------

Use multiple ``marg-list`` entries when different data sets or experiments
constrain the same parameter vector:

.. code-block:: yaml

   marg-list:
     - name: gw_event
       exe: example_gaussian.py
       args: "--outdir gw_out --conforming-output-name"
       n-chunk: 100
     - name: nuclear_constraint
       exe: example_gaussian2.py
       args: "--outdir nuclear_out --conforming-output-name"
       n-chunk: 200

By default, ``CON_PROD`` combines constraints multiplicatively.  Only change to
an additive combination if that is scientifically appropriate for your
likelihood convention.

Coordinate transformations
--------------------------

The post stage maps YAML coordinate fields to ``util_ConstructEOSPosterior.py``
flags:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - YAML field
     - Emitted flag
     - Meaning
   * - ``coords-fit``
     - ``--parameter``
     - Parameters included in the fit.
   * - ``coords-sample``
     - ``--integration-parameter-range``
     - Parameters and ranges sampled by MC integration.
   * - ``coords-implied``
     - ``--parameter-implied``
     - Parameters the coordinate module can derive from the fit and sample set;
       they are carried through post-processing, but HyperPipe does not sample
       them independently.
   * - ``coords-nofit``
     - ``--parameter-nofit``
     - Parameters that are sampled in MC integration but intentionally excluded
       from the fit itself.

Contributor guidance
--------------------

When authoring a new coordinate module, keep the split between these two fields
clear:

* use ``coords-implied`` for values your module computes from other parameters
  and that should remain available in downstream outputs;
* use ``coords-nofit`` for values you want to preserve as sampled dimensions,
  while explicitly leaving them out of the fit.

If a parameter is neither derivable by the coordinate module nor intended to be
sampled, it should not appear in either list.

Placement lanes and tracer workflow
-----------------------------------

HyperPipe placement has a few modes that produce different DAG lanes:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Configuration
     - Placement behavior
     - Expected lanes
   * - ``puff.exe`` unset/null
     - Resolve the legacy puffball executable and widen posterior-supported
       regions with ``util_HyperparameterPuffball.py``.
     - ``PUFF`` plus legacy puff/MARG_PUFF-style placement lanes, depending on
       generated DAG details.
   * - Tracer ``puff.exe`` with ``input-source: marg_net``
     - Read ``all.marg_net`` directly and write the next ``grid-{k+1}.dat``.
     - Tracer placement; no legacy ``MARG_PUFF`` grid-puff lane.
   * - Explicit custom placement executable
     - Follow the executable's contract and generated args files.
     - Inspect ``args_puff.txt`` and the generated ``*.sub`` files before
       submission.

The tracer workflow replaces the traditional posterior-plus-puff placement with
a placement updater that reads ``all.marg_net`` directly.  In the demo this is
configured by ``hyperpipe_conf_tracer.yaml``:

.. code-block:: yaml

   puff:
     exe: util_HyperparameterTracerUpdate.py
     input-source: marg_net
     puff-factor: 0.5
     force-away: 0.03
     settings:
       update-method: smc-mala-bd
       tracer-fit-method: rf
       n-mala-steps: 8
       target-ess-frac: 0.5
       birth-death-rate: 1.0

``input-source: marg_net`` is the important switch.  Without it, a tracer-aware
``puff.exe`` may be wired like a legacy puffball job and emit a ``MARG_PUFF``
lane that expects files the tracer path does not produce.

Operations and troubleshooting
==============================

Submit a generated workflow from inside the run directory:

.. code-block:: console

   $ condor_submit_dag marginalize_hyperparameters.dag

Monitor the queue:

.. code-block:: console

   $ watch condor_q

Inspect generated arguments before submission:

.. code-block:: console

   $ cat args_marg_eos.txt
   $ cat args_marg_eos_exe.txt
   $ cat args_eos_post.txt
   $ cat args_puff.txt

Common failure modes:

* ``MARG`` jobs fail immediately: check that the driver is executable, its
  environment is active, and it accepts the standard flags listed above.
* Consolidation produces empty or missing ``*.net_marg`` files: check the
  driver's output file name and confirm ``--conforming-output-name`` was used.
* The posterior collapses to a spike: inspect whether the initial grid/ranges
  cover the high-likelihood region and whether likelihood signs/scales are
  correct.
* Convergence never triggers: check that later ``grid-*.dat`` files are changing
  less over time, and consider whether the threshold is appropriate for the
  dimensionality and sampling noise.
* Tracer runs produce missing ``grid_puff`` errors: confirm
  ``puff.input-source: marg_net`` is present with the tracer updater.

For RIFT users
==============

HyperPipe generalizes the RIFT pipeline while preserving the broad workflow
shape:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Standard RIFT concept
     - HyperPipe concept
   * - ``ILE.sub``
     - ``MARG.sub``; one or more likelihood-driver lanes.
   * - ``PUFF.sub``
     - ``MARG_PUFF.sub`` in legacy placement, or tracer placement.
   * - ``CIP.sub``
     - ``EOS_POST.sub``.
   * - ``ILE.ini``
     - ``hyperpipe_conf.yaml``.
   * - ``util_RIFT_pseudo_pipe.py``
     - ``util_RIFT_hyperpipe.py``.

The practical mental shift is that HyperPipe's coordinates and likelihoods are
user-declared.  The pipeline manages adaptive placement and Condor orchestration;
the scientific meaning lives in the driver, coordinate module, priors, and
configuration.
