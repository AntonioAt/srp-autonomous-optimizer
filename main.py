import time
import random
import numpy as np

# ==============================================================================
# IMPORT MODULES FROM REPOSITORY STRUCTURE
# Assuming directory: srp-autonomous-optimizer/
# ==============================================================================
from src.diagnostic.feature_extractor import DynacardAnalytics
from src.diagnostic.evaluator import WellMonitor
from src.prescriptive.physics_solver import SRPPhysicsEngine

# ==============================================================================
# MOCK COMPONENTS FOR LOCAL TESTING (Simulating Real-World Inputs)
# ==============================================================================
class MockMLModel:
    """Simulates the trained Random Forest diagnostic model."""
    def predict(self, position, load):
        # In a real scenario, this extracts features and calls rf_model.predict()
        # Here, we randomly inject a chronic fluid pound scenario for testing
        if random.random() < 0.75:
            return 'fluid_pound'
        return 'normal'

def generate_mock_telemetry():
    """Simulates raw high-frequency telemetry data from the well's edge sensor."""
    t = np.linspace(0, 2*np.pi, 100)
    position = (1 - np.cos(t)) / 2
    # Create a rough fluid pound shape for the math to evaluate
    load = np.ones(100)
    load[50:] = 0.2 
    load += np.random.normal(0, 0.02, 100) # Add mechanical noise
    return {'position': position, 'load': load}

def execute_scada_command(well_id, command_type, value):
    """Simulates sending Modbus/MQTT commands to the field VFD."""
    print(f"\n---> [SCADA TRANSMISSION] Sending {command_type} = {value:.1f} to {well_id} VFD...")
    time.sleep(1) # Simulate network transmission latency
    print(f"---> [SCADA ACKNOWLEDGE] VFD frequency updated successfully.\n")

# ==============================================================================
# MAIN ORCHESTRATOR PIPELINE
# ==============================================================================
def main():
    print("==================================================")
    print(" SRP AUTONOMOUS CONTROL PIPELINE INITIALIZED")
    print("==================================================\n")
    
    # 1. System & Well Initialization
    WELL_ID = "BETA-07"
    current_spm = 10.0
    
    # Initialize Stage 1 (Diagnostics) & Stage 2 (Prescriptive Physics)
    monitor = WellMonitor(well_id=WELL_ID, window_size=20) # Small window for fast simulation
    physics_engine = SRPPhysicsEngine(well_id=WELL_ID, plunger_diameter=1.25, stroke_length=144)
    ml_model = MockMLModel()
    
    # Control Dynamics & Safety Variables
    in_cooldown = False
    cooldown_strokes_remaining = 0
    COOLDOWN_PERIOD = 15 # Wait 15 strokes before evaluating again
    MAX_SPM_DROP_STEP = 1.5 # SAFETY CONSTRAINT: Max SPM change per intervention to prevent well shock
    
    print(f"[{WELL_ID}] Pipeline Active. Current SPM: {current_spm}")
    print(f"[{WELL_ID}] Awaiting telemetry stream...\n")
    
    # 2. Continuous Execution Loop (The Edge Server Loop)
    stroke_counter = 0
    
    try:
        while True: # Infinite loop simulating 24/7 monitoring
            stroke_counter += 1
            
            # --- A. INGEST TELEMETRY ---
            telemetry_data = generate_mock_telemetry()
            raw_pos = telemetry_data['position']
            raw_load = telemetry_data['load']
            
            # --- B. CONTROL DYNAMICS (DEAD TIME) ---
            if in_cooldown:
                cooldown_strokes_remaining -= 1
                print(f"Stroke {stroke_counter:04d} | [SYSTEM COOLDOWN] Equalizing annulus fluid level... ({cooldown_strokes_remaining} strokes left)", end="\r")
                if cooldown_strokes_remaining <= 0:
                    print(f"\n\n[SYSTEM] Cooldown complete. Resuming active diagnostic monitoring for {WELL_ID}.\n")
                    in_cooldown = False
                time.sleep(0.1)
                continue # Skip evaluation while well is stabilizing
                
            # --- C. STAGE 1: DIAGNOSTIC ENGINE ---
            # Fast ML Classification
            pred_class = ml_model.predict(raw_pos, raw_load)
            print(f"Stroke {stroke_counter:04d} | ML Class: {pred_class.upper().ljust(15)}", end="\r")
            
            # Lazy Evaluation for Physics Severity (Only run heavy calculus if pound is detected)
            stroke_severity = 0.0
            if pred_class == 'fluid_pound':
                stroke_severity = DynacardAnalytics.calculate_fluid_pound_severity(raw_pos, raw_load)
                
            # Evaluate Rules to prevent alarm fatigue
            alarm = monitor.process_new_stroke(pred_class, severity=stroke_severity)
            
            # --- D. STAGE 2: PRESCRIPTIVE ENGINE ---
            if alarm:
                print(f"\n\n{alarm['message']}")
                
                if alarm['type'] == 'ROD_PARTED':
                    execute_scada_command(WELL_ID, 'MOTOR_POWER', 0.0)
                    print("[SYSTEM] Catastrophic failure handled. Halting execution pipeline.")
                    break # Stop the loop entirely
                    
                elif alarm['type'] == 'FLUID_POUND':
                    severity = alarm['severity_pct']
                    print(f"[ENGINEERING] Calculating dynamic mitigation strategy...")
                    
                    # Calculate absolute target SPM
                    target_spm = physics_engine.calculate_optimal_spm(current_spm, severity)
                    
                    # Implement Damped Stepwise Control (Protecting Inflow Performance)
                    spm_difference = current_spm - target_spm
                    if spm_difference > MAX_SPM_DROP_STEP:
                        recommended_spm = current_spm - MAX_SPM_DROP_STEP
                        print(f" > Warning: Target drop is too aggressive ({spm_difference:.1f} SPM). Applying damped step.")
                    else:
                        recommended_spm = target_spm
                    
                    print(f" > Current Capacity ({current_spm:.1f} SPM)    : {physics_engine.calculate_pump_displacement(current_spm):.1f} BPD")
                    print(f" > Target Capacity  ({target_spm:.1f} SPM)    : {physics_engine.calculate_pump_displacement(target_spm):.1f} BPD")
                    print(f" > Executing Step   ({recommended_spm:.1f} SPM)    : {physics_engine.calculate_pump_displacement(recommended_spm):.1f} BPD")
                    
                    # Execute Action
                    execute_scada_command(WELL_ID, 'SET_SPM', recommended_spm)
                    
                    # Update Internal State
                    current_spm = recommended_spm
                    in_cooldown = True
                    cooldown_strokes_remaining = COOLDOWN_PERIOD
                    monitor.reset_history() # Clear history to avoid double-triggering
                    
            time.sleep(0.05) # Simulate time gap between physical strokes
            
    except KeyboardInterrupt:
        print("\n\n[SYSTEM] Manual interruption received. Shutting down pipeline gracefully.")

if __name__ == "__main__":
    # Note for Laodi: Ensure your src/ directory modules are correctly placed before running.
    # If running as a standalone test, you can paste the classes from evaluator.py 
    # and physics_solver.py directly above this main() function.
    main()
