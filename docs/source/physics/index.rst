Waveform Generation
====================

This section describes the tools RIFT uses to generate gravitational-wave signals. The primary objective here is the a conceptual and workflow-oriented guide to how waveforms are produced, specifically the generation of spin-weighted spherical harmonic decompositions $h_{lm}Y_{lm}$.

Waveform Interfaces
-------------------

RIFT utilizes several interfaces to produce waveforms, depending on the required speed and accuracy:

1. **LALSimulation Utilities**: The base layer for many waveform calls, providing low-level access to the LAL library.
2. **GWSignal Interface**: A wrapper around the ``gwsignal`` library, providing a clean interface for high-fidelity waveforms.
3. **ROM Waveform Manager**: A specialized interface for Reduced Order Models (ROMs) used in high-performance iterative pipelines.

For detailed guides on each, see the following:

.. toctree::
   :maxdepth: 1

   GWSignal
   ROMWaveformManager
