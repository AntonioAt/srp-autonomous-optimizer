# SRP Autonomous Optimizer

A closed-loop automation pipeline for Sucker Rod Pump (SRP) systems. This project bridges the gap between purely data-driven Machine Learning diagnostics and rigid physical kinematic constraints to safely optimize artificial lift operations at the edge.

## The Problem
Commercial diagnostic software often stops at identifying a problem (e.g., "Fluid Pound detected"). Pure Machine Learning approaches attempt to automate solutions but often fail in the field because black-box algorithms do not understand physical constraints like Inflow Performance Relationships (IPR) or motor inertia, leading to catastrophic equipment failure or killed wells.

## The Solution: A Two-Stage Architecture
This pipeline utilizes a decoupled architecture to separate anomaly detection from mechanical intervention.

1. **Stage 1: Diagnostic Engine (ML + Signal Processing)**
   - Processes high-frequency telemetry data (Position vs. Load dynacards).
   - Utilizes a Random Forest classifier for rapid anomaly detection.
   - Applies Savitzky-Golay filtering and numerical differentiation to analytically calculate the exact volumetric severity of fluid pounds, avoiding optical illusions in raw data.
   - Implements a moving-window state machine to prevent "alarm fatigue" from transient well noise.

2. **Stage 2: Prescriptive Engine (Physics Solver + Control Dynamics)**
   - Acts only when a chronic issue is verified by Stage 1.
   - Calculates theoretical Pump Displacement (PD) and determines the exact Strokes Per Minute (SPM) reduction required to balance the formation's inflow.
   - **Operational Rigor:** Enforces *Damped Stepwise Control* (capping max SPM changes) to prevent hydrostatic shock to the reservoir. 

## Key Engineering Fail-Safes
Industrial edge-computing cannot assume perfect hardware. This orchestrator includes:
- **Two-Way SCADA Handshake:** Separates network-write commands from physical mechanical verification. The system polls the VFD until the motor inertia settles at the target RPM.
- **Dead-Time Cooldown:** Halts automated interventions after a parameter change to allow annular fluid levels to reach steady-state equilibrium.
- **Catastrophic Fallback:** Instantly issues a SCADA kill-signal upon detecting severe mechanical parting (e.g., Rod Parted) to protect the gearbox.

## Repository Structure
```text
├── src/
│   ├── diagnostic/
│   │   ├── feature_extractor.py  # Numerical differentiation & geometry extraction
│   │   └── evaluator.py          # State machine and alarm logic
│   ├── prescriptive/
│   │   └── physics_solver.py     # Kinematic calculators (API RP 11L basis)
│   └── pipeline/
│       └── scada_interface.py    # Modbus/MQTT fail-safe logic
├── main.py                       # The edge orchestrator loop
└── requirements.txt              # numpy, scipy, scikit-learn, pandas
