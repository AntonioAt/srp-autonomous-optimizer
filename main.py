import time
import random
import numpy as np
import sys
import csv  # Added for data logging

# ==============================================================================
# MOCK MODULES (For Standalone Testing in Colab/Terminal)
# ==============================================================================

class MockDynacardAnalytics:
    @staticmethod
    def calculate_fluid_pound_severity(position, load):
        # Simulates calculating the empty pump volume percentage
        return random.uniform(25.0, 40.0)

class MockWellMonitor:
    def __init__(self, well_id, window_size=10):
        self.well_id = well_id
        self.window_size = window_size
        self.stroke_history = []
        self.active_alarm = None

    def process_new_stroke(self, pred_class, severity=0.0):
        self.stroke_history.append((pred_class, severity))
        if len(self.stroke_history) > self.window_size:
            self.stroke_history.pop(0)

        # Only evaluate once the window is full
        if len(self.stroke_history) == self.window_size:
            classes = [item[0] for item in self.stroke_history]
            pound_count = classes.count('fluid_pound')
            pound_pct = (pound_count / self.window_size) * 100.0

            if pound_pct >= 70.0:
                if self.active_alarm != 'FLUID_POUND':
                    self.active_alarm = 'FLUID_POUND'
                    pound_severities = [item[1] for item in self.stroke_history if item[0] == 'fluid_pound']
                    avg_severity = np.mean(pound_severities) if pound_severities else 0.0
                    return {
                        "level": "WARNING",
                        "type": "FLUID_POUND",
                        "severity_pct": avg_severity,
                        "message": f"[WARNING] Well {self.well_id}: Chronic Fluid Pound detected. Avg Severity: {avg_severity:.1f}%"
                    }
            else:
                if self.active_alarm == 'FLUID_POUND':
                    self.active_alarm = None
                    return {
                        "level": "INFO",
                        "type": "NORMAL",
                        "message": f"\n[INFO] Well {self.well_id}: Conditions normalized. Alarms cleared."
                    }
        return None

    def reset_history(self):
        self.stroke_history.clear()
        self.active_alarm = None

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
    def __init__(self):
        self.stroke_count = 0
        
    def predict(self, position, load):
        self.stroke_count += 1
        
        # Real-world dynamic scenario simulation
        if self.stroke_count < 15:
            return 'normal'
        elif 15 <= self.stroke_count < 40:
            return 'fluid_pound'
        else:
            if random.random() < 0.85:
                return 'normal'
            else:
                return 'fluid_pound'

def generate_mock_telemetry():
    """Simulates incoming high-frequency sensor data."""
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
        
        network_success = False
        for attempt in range(1, self.max_retries + 1):
            try:
                print(f"   -> Network Write (Attempt {attempt}/{self.max_retries})...", end=" ")
                time.sleep(0.5) 
                if random.random() < 0.1: # 10% chance of network failure
                    raise ConnectionError("Packet Loss")
                print("SUCCESS")
                network_success = True
                break
            except ConnectionError:
                print("FAILED (Network Timeout)")
                time.sleep(1) 
                
        if not network_success:
            print("[SCADA FATAL] Lost communication with VFD controller.")
            return False

        print("   -> Verifying motor mechanical response...")
        for i in range(3): 
            time.sleep(0.8)
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
    
    WELL_ID = "BETA-07"
    current_spm = 10.0
    
    monitor = MockWellMonitor(well_id=WELL_ID, window_size=10)
    physics_engine = MockSRPPhysicsEngine(well_id=WELL_ID)
    scada = SCADAInterface(well_id=WELL_ID)
    ml_model = MockMLModel()
    
    in_cooldown = False
    cooldown_strokes_remaining = 0
    COOLDOWN_PERIOD = 12
    MAX_SPM_DROP_STEP = 1.5 
    is_system_healthy = True 
    stroke_counter = 0
    
    # --------------------------------------------------------------------------
    # INITIALIZE CSV LOG FILE (Write Headers)
    # --------------------------------------------------------------------------
    log_filename = 'srp_operation_log.csv'
    with open(log_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['stroke_number', 'current_spm', 'pound_severity_pct'])
    
    print(f"[{WELL_ID}] Pipeline Active. Current SPM: {current_spm}")
    print(f"[{WELL_ID}] Logging data to {log_filename}")
    print(f"[{WELL_ID}] Listening to high-frequency telemetry stream...\n")
    
    try:
        while is_system_healthy and stroke_counter < 80: 
            stroke_counter += 1
            telemetry = generate_mock_telemetry()
            
            # 1. Control Dynamics: Dead Time Evaluation
            if in_cooldown:
                cooldown_strokes_remaining -= 1
                sys.stdout.write(f"\rStroke {stroke_counter:04d} | [SYSTEM] Cooldown active. Equalizing annulus fluid ({cooldown_strokes_remaining} strokes left)...")
                sys.stdout.flush()
                
                # Log the cooldown state (severity is nominally 0 during stabilization)
                with open(log_filename, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([stroke_counter, current_spm, 0.0])
                
                if cooldown_strokes_remaining <= 0:
                    print(f"\n\n[SYSTEM] Cooldown complete. Resuming active diagnostic monitoring.")
                    in_cooldown = False
                time.sleep(0.2)
                continue 
                
            # 2. Diagnostic Stage
            pred_class = ml_model.predict(telemetry['position'], telemetry['load'])
            sys.stdout.write(f"\rStroke {stroke_counter:04d} | ML Class: {pred_class.upper().ljust(15)}")
            sys.stdout.flush()
            
            stroke_severity = 0.0
            if pred_class == 'fluid_pound':
                stroke_severity = MockDynacardAnalytics.calculate_fluid_pound_severity(telemetry['position'], telemetry['load'])
                
            # ------------------------------------------------------------------
            # APPEND DATA TO CSV LOG
            # ------------------------------------------------------------------
            with open(log_filename, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([stroke_counter, current_spm, stroke_severity])
                
            alarm = monitor.process_new_stroke(pred_class, severity=stroke_severity)
            
            # 3. Prescriptive Stage & Execution
            if alarm:
                if alarm['level'] == 'INFO':
                    print(f"{alarm['message']}")
                else:
                    print(f"\n\n{alarm['message']}")
                    
                    if alarm['type'] == 'ROD_PARTED':
                        scada.emergency_shutdown()
                        is_system_healthy = False 
                        break 
                        
                    elif alarm['type'] == 'FLUID_POUND':
                        print(f"[ENGINEERING] Calculating dynamic mitigation strategy...")
                        target_spm = physics_engine.calculate_optimal_spm(current_spm, alarm['severity_pct'])
                        
                        if (current_spm - target_spm) > MAX_SPM_DROP_STEP:
                            recommended_spm = current_spm - MAX_SPM_DROP_STEP
                            print(f" > Warning: Target drop is too aggressive. Applying damped step of max {MAX_SPM_DROP_STEP} SPM.")
                        else:
                            recommended_spm = target_spm
                        
                        print(f" > Current Capacity ({current_spm:.1f} SPM) : {physics_engine.calculate_pump_displacement(current_spm):.1f} BPD")
                        print(f" > Target Capacity  ({target_spm:.1f} SPM) : {physics_engine.calculate_pump_displacement(target_spm):.1f} BPD")
                        print(f" > Executing Step   ({recommended_spm:.1f} SPM) : {physics_engine.calculate_pump_displacement(recommended_spm):.1f} BPD")
                        
                        action_success = scada.set_and_verify_spm(recommended_spm)
                        
                        if action_success:
                            current_spm = recommended_spm
                            in_cooldown = True
                            cooldown_strokes_remaining = COOLDOWN_PERIOD
                            monitor.reset_history() 
                        else:
                            print(f"\n[SYSTEM FATAL] Cannot mitigate fluid pound due to SCADA failure!")
                            scada.emergency_shutdown()
                            is_system_healthy = False
            
            time.sleep(0.2) 
            
        print("\n\n[SYSTEM] Simulation ended.")

    except KeyboardInterrupt:
        print("\n\n[SYSTEM] Manual interruption received. Shutting down pipeline gracefully.")

if __name__ == "__main__":
    main()
