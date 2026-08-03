EOS posterior pipeline
======================

The ``create_eos_posterior_pipeline`` executable builds an iterative workflow
for combining event likelihood information with an equation-of-state (EOS)
grid.  It writes workflow directories and backend submission artefacts; it
does not itself submit a Condor job.  The EOS representation, physical checks,
and helper classes are documented separately in :doc:`physics/EOSManager`.

Inputs
------

The driver requires an initial, plain-text EOS grid through ``--input-grid``;
the input is copied into the working directory as ``grid-0.dat``.  Provide one
``--event-file`` for each event likelihood input.  The production interface
also needs an EOS-posterior argument file via ``--eos-post-args``.  The
event-marginalization configuration can be supplied as a shared
``--marg-event-args`` file or as matching per-event argument and executable
list files.

For example, the repository's backend demo exercises the list-file form:

.. code-block:: console

   create_eos_posterior_pipeline \
       --working-directory eos-run \
       --input-grid eos-grid.dat \
       --event-file event-1.net \
       --event-file event-2.net \
       --marg-event-args-list-file args_marg_event_list.txt \
       --marg-event-exe-list-file marg_event_exe_list.txt \
       --marg-event-nchunk-list-file marg_event_nchunk_list.txt \
       --eos-post-args args_eos_post.txt \
       --n-iterations 2

The list files must correspond to the supplied event files.  The executable
copies event inputs into the working directory with standardized names, so use
a new or otherwise disposable working directory for each generated workflow.

Generated workflow
------------------

For every configured iteration, the driver creates marginalization, posterior,
and consolidation directories, along with logs and scheduler submit files.
Typical top-level results include:

* ``grid-0.dat`` and later grid files for EOS samples;
* ``iteration_*_marg/``, ``iteration_*_post/``, and ``iteration_*_con/``;
* ``all.marg_net``, the accumulated evaluated likelihood table; and
* a top-level DAG and backend-specific submission files.

Optional puff/tracer and convergence-test inputs add their corresponding
workflow nodes.  Flags such as ``--use-osg`` and ``--use-singularity`` change
how file paths and execution environments are prepared; use them only on a
site configured for those modes.

Validate before submission
--------------------------

Start by checking the installed executable's available options:

.. code-block:: console

   create_eos_posterior_pipeline --help

The representative non-submitting contract is
``MonteCarloMarginalizeCode/Code/test/backends/demo_create_eos_posterior_pipeline.sh``.
It generates workflow artefacts for supported backends from small fixtures;
it is useful for checking a local installation before adapting the command to
scientific inputs.  Review the produced files and their arguments before
submitting the top-level DAG through the scheduler appropriate to the site.

For the older HyperPipe-oriented interface and its directory conventions, see
:doc:`hyperpipe`.
