import time
import random
import numpy as np
import sys
import csv
import threading

# ==============================================================================
# MOCK MODULES (For Standalone Testing / Portfolio Demonstration)
# ==============================================================================

class MockDynacardAnalytics:
    @staticmethod
    def calculate_fluid_pound_severity(position, load):
        """Simulates the analytical calculation of empty pump volume (%)."""
        return random.uniform(25.0, 40.0)

class MockWellMonitor:
    def __init__(self, well_id, window_size=10):
        self.well_id = well_id
        self.window_size = window_size
        self.stroke_history = []
        self.active_alarm = None

    def process_new_stroke(self, pred_class, severity=0.0):
        """Processes stroke classifications and prevents alarm fatigue."""
        self.stroke_history.append((pred_class, severity))
        if len(self.stroke_history) > self.window_size:
            self.stroke_history.pop(0)

        # Only evaluate once the moving window is full
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
        """Calculates theoretical Pump Displacement in Barrels Per Day (BPD)."""
        return self.constant * spm * self.S * (self.D ** 2)

    def calculate_optimal_spm(self, current_spm, severity_pct):
        """Calculates required SPM reduction based on volumetric severity."""
        current_pd = self.calculate_pump_displacement(current_spm)
        target_pd = current_pd * (1.0 - (severity_pct / 100.0))
        new_spm = target_pd / (self.constant * self.S * (self.D ** 2))
        return max(new_spm, 3.0) # Hardware Safety Constraint: Motor cannot drop below 3.0 SPM

class MockMLModel:
    def __init__(self):
        self.stroke_count = 0
        # True Randomness: The anomaly will start at an unpredictable time between stroke 10 and 50.
        self.pound_start_stroke = random.randint(10, 50) 
        
    def predict(self, position, load):
        """Simulates the Random Forest classification output over time with chaotic behavior."""
        self.stroke_count += 1
        
        # Phase 1: Normal steady-state operation
        if self.stroke_count < self.pound_start_stroke:
            return 'normal'
        else:
            # Phase 2: Inflow drops unpredictably (Chaos Theory)
            # 60% chance of fluid pound, 40% chance of normal stroke
            if random.random() < 0.60:
                return 'fluid_pound'
            else:
                return 'normal'

def generate_mock_telemetry():
    """Simulates high-frequency raw edge sensor data."""
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
        """Executes a two-way handshake to ensure physical motor compliance."""
        print(f"\n[SCADA] Initiating physical intervention for {self.well_id}. Target: {target_spm:.1f} SPM")
        
        network_success = False
        for attempt in range(1, self.max_retries + 1):
            try:
                print(f"   -> Network Write (Attempt {attempt}/{self.max_retries})...", end=" ")
                time.sleep(0.5) 
                if random.random() < 0.1: # 10% chance of packet loss
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
# ASYNCHRONOUS HUMAN-IN-THE-LOOP (Control Room Override)
# ==============================================================================
manual_override_triggered = False

def listen_for_operator_override():
    """
    Runs in a daemon background thread. Listens for human emergency input 
    without blocking the high-frequency telemetry reading loop.
    """
    global manual_override_triggered
    while True:
        try:
            command = input()
            if command.strip().upper() == 'STOP':
                manual_override_triggered = True
                break
        except EOFError:
            break

# ==============================================================================
# MAIN ORCHESTRATOR PIPELINE
# ==============================================================================
def main():
    print("==========================================================")
    print(" SRP AUTONOMOUS CONTROL EDGE-SERVER INITIALIZED")
    print("==========================================================\n")
    
    WELL_ID = "BETA-07"
    current_spm = 10.0
    
    # Instantiate modules
    monitor = MockWellMonitor(well_id=WELL_ID, window_size=10)
    physics_engine = MockSRPPhysicsEngine(well_id=WELL_ID)
    scada = SCADAInterface(well_id=WELL_ID)
    ml_model = MockMLModel()
    
    # Control Dynamics Parameters
    in_cooldown = False
    cooldown_strokes_remaining = 0
    COOLDOWN_PERIOD = 12
    MAX_SPM_DROP_STEP = 1.5 
    is_system_healthy = True 
    stroke_counter = 0
    
    # --------------------------------------------------------------------------
    # INITIALIZE CSV LOG FILE (For Dashboarding & ROI Proof)
    # --------------------------------------------------------------------------
    log_filename = 'srp_operation_log.csv'
    with open(log_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['stroke_number', 'current_spm', 'pound_severity_pct'])
    
    print(f"[{WELL_ID}] Pipeline Active. Current SPM: {current_spm}")
    print(f"[{WELL_ID}] Logging data to {log_filename}")
    
    # Start the Asynchronous Listener
    print(f"[{WELL_ID}] MANUAL OVERRIDE ACTIVE: Type 'STOP' and press Enter at any time to kill the motor.\n")
    override_thread = threading.Thread(target=listen_for_operator_override, daemon=True)
    override_thread.start()
    
    try:
        # The Infinite Edge-Computing Loop (Capped at 100 strokes for Colab simulation)
        while is_system_healthy and stroke_counter < 100: 
            
            # --- 1. ASYNCHRONOUS SAFETY CHECK ---
            global manual_override_triggered
            if manual_override_triggered:
                print("\n\n[CONTROL ROOM] MANUAL OVERRIDE COMMAND RECEIVED!")
                scada.emergency_shutdown()
                is_system_healthy = False
                break 
                
            stroke_counter += 1
            telemetry = generate_mock_telemetry()
            
            # --- 2. CONTROL DYNAMICS: DEAD TIME EVALUATION ---
            if in_cooldown:
                cooldown_strokes_remaining -= 1
                sys.stdout.write(f"\rStroke {stroke_counter:04d} | [SYSTEM] Cooldown active. Equalizing annulus fluid ({cooldown_strokes_remaining} strokes left)...")
                sys.stdout.flush()
                
                with open(log_filename, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([stroke_counter, current_spm, 0.0])
                
                if cooldown_strokes_remaining <= 0:
                    print(f"\n\n[SYSTEM] Cooldown complete. Resuming active diagnostic monitoring.")
                    in_cooldown = False
                time.sleep(0.2)
                continue 
                
            # --- 3. DIAGNOSTIC STAGE (Machine Learning + Calculus) ---
            pred_class = ml_model.predict(telemetry['position'], telemetry['load'])
            sys.stdout.write(f"\rStroke {stroke_counter:04d} | ML Class: {pred_class.upper().ljust(15)}")
            sys.stdout.flush()
            
            stroke_severity = 0.0
            if pred_class == 'fluid_pound':
                stroke_severity = MockDynacardAnalytics.calculate_fluid_pound_severity(telemetry['position'], telemetry['load'])
                
            with open(log_filename, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([stroke_counter, current_spm, stroke_severity])
                
            alarm = monitor.process_new_stroke(pred_class, severity=stroke_severity)
            
            # --- 4. PRESCRIPTIVE STAGE & SCADA EXECUTION ---
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
                        
                        # Apply Damped Stepwise Control to protect Inflow Performance
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
            
            time.sleep(0.2) # Pacing for visualization
            
        if is_system_healthy:
            print("\n\n[SYSTEM] Simulation ended naturally.")

    except KeyboardInterrupt:
        print("\n\n[SYSTEM] Manual interruption received via KeyboardInterrupt. Shutting down pipeline gracefully.")

if __name__ == "__main__":
    main()
