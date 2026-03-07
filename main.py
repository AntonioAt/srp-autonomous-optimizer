import time
import random
import numpy as np
import sys

# ==============================================================================
# MOCK MODULES (For Standalone Testing)
# In production, these would be imported from your 'src/' directory:
# from src.diagnostic.feature_extractor import DynacardAnalytics
# from src.diagnostic.evaluator import WellMonitor
# from src.prescriptive.physics_solver import SRPPhysicsEngine
# ==============================================================================

class MockDynacardAnalytics:
    @staticmethod
    def calculate_fluid_pound_severity(position, load):
        # Simulates calculating empty pump volume percentage (0-100%)
        return random.uniform(20.0, 45.0)

class MockWellMonitor:
    def __init__(self, well_id):
        self.well_id = well_id
        self.stroke_count = 0
        
    def process_new_stroke(self, pred_class, severity=0.0):
        self.stroke_count += 1
        # Force a fluid pound alarm on the 5th stroke for testing purposes
        if self.stroke_count == 5:
            return {
                "level": "WARNING",
                "type": "FLUID_POUND",
                "severity_pct": severity,
                "message": f"[WARNING] Well {self.well_id}: Chronic Fluid Pound detected. Avg Severity: {severity:.1f}%"
            }
        return None
        
    def reset_history(self):
        self.stroke_count = 0

class MockSRPPhysicsEngine:
    def __init__(self, well_id):
        self.constant = 0.1166
        self.S = 144  # Stroke Length (inches)
        self.D = 1.25 # Pump Diameter (inches)

    def calculate_pump_displacement(self, spm):
        return self.constant * spm * self.S * (self.D ** 2)

    def calculate_optimal_spm(self, current_spm, severity_pct):
        current_pd = self.calculate_pump_displacement(current_spm)
        target_pd = current_pd * (1.0 - (severity_pct / 100.0))
        new_spm = target_pd / (self.constant * self.S * (self.D ** 2))
        return max(new_spm, 3.0) # Ensure SPM doesn't drop below motor minimum

class MockMLModel:
    def predict(self, position, load):
        return 'fluid_pound'

def generate_mock_telemetry():
    """Simulates incoming sensor data from the edge device."""
    return {'position': np.array([0, 0.5, 1.0, 0.5, 0]), 'load': np.array([0, 1.0, 1.0, 0.2, 0])}

# ==============================================================================
# SCADA INTERFACE (The Fail-Safe Network Layer)
# ==============================================================================
class SCADAInterface:
    def __init__(self, well_id, max_retries=3, physical_timeout=10):
        self.well_id = well_id
        self.max_retries = max_retries
        self.physical_timeout = physical_timeout

    def set_and_verify_spm(self, target_spm):
        print(f"\n[SCADA] Initiating physical intervention for {self.well_id}. Target: {target_spm:.1f} SPM")
        
        # Phase 1: Network Transmission & Retries
        network_success = False
        for attempt in range(1, self.max_retries + 1):
            try:
                print(f"   -> Network Write (Attempt {attempt}/{self.max_retries})...", end=" ")
                time.sleep(0.5) # Simulate latency
                
                # 15% chance of network failure to test robustness
                if random.random() < 0.15: 
                    raise ConnectionError("Packet Loss")
                    
                print("SUCCESS")
                network_success = True
                break
            except ConnectionError:
                print("FAILED (Network Timeout)")
                time.sleep(1) # Backoff
                
        if not network_success:
            print("[SCADA FATAL] Lost communication with VFD controller.")
            return False

        # Phase 2: Physical Verification (Motor Inertia Simulation)
        print("   -> Verifying motor mechanical response...")
        for i in range(3): 
            time.sleep(1)
            print(f"      ... Waiting for motor RPM to stabilize ({(i+1)*33}%)", end="\r")
        
        print(f"\n   -> [SCADA VERIFIED] Motor stabilized at {target_spm:.1f} SPM.")
        return True

    def emergency_shutdown(self):
        print(f"\n[SCADA CRITICAL] Sending KILL SIGNAL to {self.well_id} motor immediately!")
        time.sleep(1)
        print("[SCADA CRITICAL] Motor power cut off. Well is secured.")

# ==============================================================================
# MAIN ORCHESTRATOR PIPELINE
# ==============================================================================
def main():
    print("==========================================================")
    print(" SRP AUTONOMOUS CONTROL EDGE-SERVER INITIALIZED")
    print("==========================================================\n")
    
    # 1. Pipeline Initialization
    WELL_ID = "BETA-07"
    current_spm = 10.0
    
    # Instantiate modules
    monitor = MockWellMonitor(well_id=WELL_ID)
    physics_engine = MockSRPPhysicsEngine(well_id=WELL_ID)
    scada = SCADAInterface(well_id=WELL_ID)
    ml_model = MockMLModel()
    
    # Control Dynamics & Safety Parameters
    in_cooldown = False
    cooldown_strokes_remaining = 0
    COOLDOWN_PERIOD = 8
    MAX_SPM_DROP_STEP = 1.5 
    is_system_healthy = True 
    
    print(f"[{WELL_ID}] Pipeline Active. Current SPM: {current_spm}")
    print(f"[{WELL_ID}] Listening to high-frequency telemetry stream...\n")
    
    stroke_counter = 0
    
    try:
        # The Infinite Loop (Edge Computing Paradigm)
        while is_system_healthy:
            stroke_counter += 1
            
            # A. Ingest Data
            telemetry = generate_mock_telemetry()
            
            # B. Control Dynamics: Dead Time Evaluation
            if in_cooldown:
                cooldown_strokes_remaining -= 1
                sys.stdout.write(f"\rStroke {stroke_counter:04d} | [SYSTEM] Cooldown active. Equalizing annulus fluid ({cooldown_strokes_remaining} strokes left)...")
                sys.stdout.flush()
                
                if cooldown_strokes_remaining <= 0:
                    print(f"\n\n[SYSTEM] Cooldown complete. Resuming active diagnostic monitoring.")
                    in_cooldown = False
                time.sleep(0.5)
                continue 
                
            # C. Diagnostic Stage
            pred_class = ml_model.predict(telemetry['position'], telemetry['load'])
            sys.stdout.write(f"\rStroke {stroke_counter:04d} | ML Class: {pred_class.upper().ljust(15)}")
            sys.stdout.flush()
            
            stroke_severity = 0.0
            if pred_class == 'fluid_pound':
                stroke_severity = MockDynacardAnalytics.calculate_fluid_pound_severity(telemetry['position'], telemetry['load'])
                
            alarm = monitor.process_new_stroke(pred_class, severity=stroke_severity)
            
            # D. Prescriptive Stage & Execution
            if alarm:
                print(f"\n\n{alarm['message']}")
                
                if alarm['type'] == 'ROD_PARTED':
                    scada.emergency_shutdown()
                    is_system_healthy = False 
                    break 
                    
                elif alarm['type'] == 'FLUID_POUND':
                    severity = alarm['severity_pct']
                    print(f"[ENGINEERING] Calculating dynamic mitigation strategy...")
                    
                    target_spm = physics_engine.calculate_optimal_spm(current_spm, severity)
                    
                    # Apply Damped Stepwise Control to protect IPR
                    if (current_spm - target_spm) > MAX_SPM_DROP_STEP:
                        recommended_spm = current_spm - MAX_SPM_DROP_STEP
                        print(f" > Warning: Target drop is too aggressive. Applying damped step of max {MAX_SPM_DROP_STEP} SPM.")
                    else:
                        recommended_spm = target_spm
                    
                    print(f" > Current Capacity ({current_spm:.1f} SPM) : {physics_engine.calculate_pump_displacement(current_spm):.1f} BPD")
                    print(f" > Target Capacity  ({target_spm:.1f} SPM) : {physics_engine.calculate_pump_displacement(target_spm):.1f} BPD")
                    print(f" > Executing Step   ({recommended_spm:.1f} SPM) : {physics_engine.calculate_pump_displacement(recommended_spm):.1f} BPD")
                    
                    # E. The Fail-Safe SCADA Handshake
                    action_success = scada.set_and_verify_spm(recommended_spm)
                    
                    if action_success:
                        # State is ONLY updated if physical verification passes
                        current_spm = recommended_spm
                        in_cooldown = True
                        cooldown_strokes_remaining = COOLDOWN_PERIOD
                        monitor.reset_history() 
                    else:
                        # ENGINEERING DECISION: SCADA failed during a severe fluid pound.
                        # Do not let the well destroy itself. Trigger emergency shutdown.
                        print(f"\n[SYSTEM FATAL] Cannot mitigate fluid pound due to SCADA failure!")
                        scada.emergency_shutdown()
                        is_system_healthy = False # Breaks the loop
            
            time.sleep(0.5) # Simulate time gap between physical strokes
            
    except KeyboardInterrupt:
        print("\n\n[SYSTEM] Manual interruption received. Shutting down pipeline gracefully.")

if __name__ == "__main__":
    main()
