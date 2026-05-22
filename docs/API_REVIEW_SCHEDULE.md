# RIFT API Documentation Review Schedule

## Overview
This document tracks the human review process for the RIFT API documentation. Each section must be reviewed and signed off before the documentation is considered complete.

## Signoff Tracking

| Section | Status | Reviewer | Date Signed Off | Notes |
|---------|--------|---------|----------------|-------|
| Integrators (MC Sampling) | PENDING | | | |
| Interpolators (Surrogates) | PENDING | | |
| Likelihood and Priors | PENDING | | |
| Physics Utilities | PENDING | | |
| Plot Utilities | PENDING | | |
| Simulation Manager | PENDING | | |
| Calibration Marginalization | PENDING | | |
| Misc Utilities | PENDING | | |
| LALSimUtils | PENDING | | |

## Review Order and Rationale

### Phase 1: Foundational Modules (Priority: Highest)
These are the core modules that other modules depend on.

1. **Likelihood and Priors** - This is the core compute engine
   - Location: `docs/build/html/api_reference/likelihood/index.html`
   - Rationale: Understanding likelihood computation is fundamental to RIFT

2. **Physics Utilities** - Physical models
   - Location: `docs/build/html/api_reference/physics/index.html`
   - Rationale: Physics models are used by likelihood

### Phase 2: Integration Engines (Priority: High)
These are the workhorses that do the heavy lifting.

3. **Integrators (MC Sampling)** - Monte Carlo integration
   - Location: `docs/build/html/api_reference/integrators/index.html`
   - Rationale: Main inference engine, heavily used

4. **Interpolators (Surrogates)** - Surrogate models
   - Location: `docs/build/html/api_reference/interpolators/index.html`
   - Rationale: Accelerates likelihood evaluation

### Phase 3: Utilities and Visualization (Priority: Medium)
Supporting modules.

5. **Plot Utilities** - Visualization
   - Location: `docs/build/html/api_reference/plot_utilities/index.html`
   - Rationale: Used for results visualization

6. **Simulation Manager** - Simulation management
   - Location: `docs/build/html/api_reference/simulation_manager/index.html`
   - Rationale: Manages simulation workflows

### Phase 4: Specialized Modules (Priority: Lower)
Niche modules for specific use cases.

7. **Calibration Marginalization** - Calibration
   - Location: `docs/build/html/api_reference/calmarg/index.html`
   - Rationale: Calibration uncertainty

8. **Misc Utilities** - General utilities
   - Location: `docs/build/html/api_reference/misc/index.html`
   - Rationale: Various helper functions

9. **LALSimUtils** - LALSimulation interface
   - Location: `docs/build/html/api_reference/lalsimutils.html`
   - Rationale: External library interface

## Review Criteria

For each section, verify:

1. **Completeness**: Are all expected modules documented?
2. **Accuracy**: Is the documentation correct?
3. **Clarity**: Is the documentation understandable?
4. **Examples**: Are there useful examples?
5. **Cross-references**: Are related modules linked?

## Signoff Process

To sign off a section:
1. Review the section in the built HTML
2. Check all review criteria
3. Add your name, date, and notes to the table above
4. Mark the section as "APPROVED" in the Status column

## File Location

- Built HTML: `docs/build/html/`
- Source RST: `docs/source/api_reference/`
- This schedule: `docs/API_REVIEW_SCHEDULE.md`