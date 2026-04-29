Common Workflows
=================

This section describes the practical processes for deploying RIFT analyses using the ``asimov`` framework.

RIFT+Asimov Workflow
--------------------

The deployment of a RIFT analysis in ``asimov`` follows a hierarchical "Blueprint" pattern. Instead of defining every single parameter for every event, settings are split into **Common Defaults** and **Analysis-Specific Specifications**.

The final configuration applied to ``rift.ini`` is a merge of these two layers.

### 1. The Common Defaults (Infrastructure Layer)
These settings are typically defined in a global defaults file (e.g., ``production-pe-o4b.yaml``). They describe the *infrastructure* and *hardware* requirements that remain constant across different physics models.

**Typical settings in this block:**
* **Scheduler/Cluster**: OSG flags, accounting groups, and requested memory.
* **Containers**: The Singularity image path for the RIFT environment.
* **Cluster Hardware**: Lists of approved GPU architectures (e.g., Tesla P100, V100).
* **Global Sampler Defaults**: General fitting methods (e.g., ``fitting method: rf``) and default effective sample counts (``n eff: 10``).

### 2. The Analysis Specification (Physics Layer)
These settings are defined in an analysis-specific file (e.g., ``analysis_rift_SEOBNRv5PHM.yaml``). They describe the *physics* and *analysis strategy* for a specific waveform model.

**Typical settings in this block:**
* **Waveform Model**: The specific approximant (e.g., ``SEOBNRv5PHM``).
* **Physics Assumptions**: Whether the analysis assumes precession, eccentricity, or specific frequency cut-offs.
* **Model-Specific Resources**: A specialized Singularity image tailored for that specific waveform.
* **Analysis-Specific Sampler Settings**: The exact sampling method (e.g., ``sampling method: AV``) and requested disk space for that specific model's requirements.

### The Merge Process
When a user runs ``asimov apply``, ``asimov`` merges these blocks. The Analysis Specification overrides the Common Defaults where they overlap.

**Workflow Summary:**
1. **Apply Defaults**: ``asimov apply -f defaults/production-pe.yaml`` $\rightarrow$ sets up the OSG and cluster environment.
2. **Apply Analysis**: ``asimov apply -f analyses/rift-bbh/analysis_rift_SEOBNRv5PHM.yaml -e <EventID>`` $\rightarrow$ defines the specific physics and waveform for that event.
3. **Build and Submit**: ``asimov manage build`` merges these layers into a final ``rift.ini`` and submits the Condor DAG.
