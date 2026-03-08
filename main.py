import time
import random
import numpy as np
import sys
import csv
import threading
import json
import os

# ==============================================================================
# CONFIGURATION MANAGEMENT (The "User Input" Layer)
# ==============================================================================
def load_or_create_config(config_file="well_config.json"):
    """
    Loads well-specific parameters dynamically. If the file doesn't exist,
    it generates a template. This separates hardware specs from software logic.
    """
    if not os.path.exists(config_file):
        print(f"[SYSTEM] Configuration file '{config_file}' not found. Generating default template...")
        default_config = {
            "well_id": "BETA-07",
            "pump_diameter_inches": 1.25,
            "stroke_length_inches": 144.0,
            "initial_spm": 10.0,
            "motor_minimum_spm": 3.0,
            "max_spm_drop_step": 1.5,
            "cooldown_period_strokes": 12,
            "alarm_threshold_pct": 70.0
        }
        with open(config_file, 'w') as f:
            json.dump(default_config, f, indent=4)
        return default_config
    else:
        print(f"[SYSTEM] Loading dynamic well parameters from '{config_file}'...")
        with open(config_file, 'r') as f:
            return json.load(f)

# ==============================================================================
# EDGE ANALYTICS & DIAGNOSTICS
# ==============================================================================
class DynacardAnalytics:
    @staticmethod
    def calculate_fluid_pound_severity(position, load):
        """Simulates the analytical calculation of empty pump volume (%)."""
        return random.uniform(25.0, 40.0)

class WellMonitor:
    def __init__(self, well_id, threshold_pct, window_size=10):
        self.well_id = well_id
        self.window_size = window_size
        self.threshold_pct = threshold_pct
        self.stroke_history = []
        self.active_alarm = None

    def process_new_stroke(self, pred_class, severity=0.0):
        """Processes stroke classifications via a moving window to prevent alarm fatigue."""
        self.stroke_history.append((pred_class, severity))
        if len(self.stroke_history) > self.window_size:
            self.stroke_history.pop(0)

        if len(self.stroke_history) == self.window_size:
            classes = [item[0] for item in self.stroke_history]
            pound_pct = (classes.count('fluid_pound') / self.window_size) * 100.0

            if pound_pct >= self.threshold_pct:
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

# ==============================================================================
# PHYSICS & PRESCRIPTIVE CONTROL
# ==============================================================================
class SRPPhysicsEngine:
    """
    Calculates physical parameters dynamically based on injected config data,
    proving this is not a static script but a scalable engineering engine.
    """
    def __init__(self, well_id, plunger_diameter, stroke_length, min_spm):
        self.well_id = well_id
        self.constant = 0.1166
        self.D = plunger_diameter
        self.S = stroke_length
        self.MIN_SPM = min_spm

    def calculate_pump_displacement(self, spm):
        """Calculates theoretical Pump Displacement (PD) in Barrels Per Day (BPD)."""
        return self.constant * spm * self.S * (self.D ** 2)

    def calculate_optimal_spm(self, current_spm, severity_pct):
        """Derives the required SPM reduction to balance fluid inflow."""
        current_pd = self.calculate_pump_displacement(current_spm)
        target_pd = current_pd * (1.0 - (severity_pct / 100.0))
        new_spm = target_pd / (self.constant * self.S * (self.D ** 2))
        return max(new_spm, self.MIN_SPM) 

# ==============================================================================
# MOCK MACHINE LEARNING MODEL (Chaos Generator)
# ==============================================================================
class MockMLModel:
    def __init__(self):
        self.stroke_count = 0
        self.pound_start_stroke = random.randint(10, 50) 
        
    def predict(self, position, load):
        """Injects true randomness to simulate unpredictable reservoir behavior."""
        self.stroke_count += 1
        if self.stroke_count < self.pound_start_stroke:
            return 'normal'
        else:
            return 'fluid_pound' if random.random() < 0.60 else 'normal'

def generate_mock_telemetry():
    return {'position': np.array([0, 0.5, 1.0, 0.5, 0]), 'load': np.array([0, 1.0, 1.0, 0.2, 0])}

# ==============================================================================
# SCADA INTERFACE & ASYNC HUMAN OVERRIDE
# ==============================================================================
class SCADAInterface:
    def __init__(self, well_id, max_retries=3):
        self.well_id = well_id
        self.max_retries = max_retries

    def set_and_verify_spm(self, target_spm):
        print(f"\n[SCADA] Initiating physical intervention for {self.well_id}. Target: {target_spm:.1f} SPM")
        network_success = False
        for attempt in range(1, self.max_retries + 1):
            try:
                print(f"   -> Network Write (Attempt {attempt}/{self.max_retries})...", end=" ")
                time.sleep(0.5) 
                if random.random() < 0.1: raise ConnectionError("Packet Loss")
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
            time.sleep(0.6)
            print(f"      ... Waiting for motor RPM to stabilize ({(i+1)*33}%)", end="\r")
        print(f"\n   -> [SCADA VERIFIED] Motor stabilized at {target_spm:.1f} SPM.")
        return True

    def emergency_shutdown(self):
        print(f"\n[SCADA CRITICAL] Sending KILL SIGNAL to {self.well_id} motor immediately!")
        print("[SCADA CRITICAL] Motor power cut off. Well is secured.")

manual_override_triggered = False

def listen_for_operator_override():
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
    
    # 1. DYNAMIC CONFIGURATION INJECTION
    config = load_or_create_config()
    
    WELL_ID = config["well_id"]
    current_spm = config["initial_spm"]
    MAX_SPM_DROP_STEP = config["max_spm_drop_step"]
    COOLDOWN_PERIOD = config["cooldown_period_strokes"]
    
    # Instantiate modules dynamically based on config
    monitor = WellMonitor(well_id=WELL_ID, threshold_pct=config["alarm_threshold_pct"])
    physics_engine = SRPPhysicsEngine(
        well_id=WELL_ID, 
        plunger_diameter=config["pump_diameter_inches"], 
        stroke_length=config["stroke_length_inches"],
        min_spm=config["motor_minimum_spm"]
    )
    scada = SCADAInterface(well_id=WELL_ID)
    ml_model = MockMLModel()
    
    in_cooldown = False
    cooldown_strokes_remaining = 0
    is_system_healthy = True 
    stroke_counter = 0
    
    # 2. LOGGING SETUP
    log_filename = 'srp_operation_log.csv'
    with open(log_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['stroke_number', 'current_spm', 'pound_severity_pct'])
    
    print(f"[{WELL_ID}] Pipeline Active. Current SPM: {current_spm}")
    print(f"[{WELL_ID}] Pump Specs: {config['pump_diameter_inches']}\" Diameter, {config['stroke_length_inches']}\" Stroke")
    print(f"[{WELL_ID}] MANUAL OVERRIDE ACTIVE: Type 'STOP' and press Enter to kill the motor.\n")
    
    override_thread = threading.Thread(target=listen_for_operator_override, daemon=True)
    override_thread.start()
    
    try:
        # 3. EDGE CONTROL LOOP
        while is_system_healthy and stroke_counter < 100: 
            
            global manual_override_triggered
            if manual_override_triggered:
                print("\n\n[CONTROL ROOM] MANUAL OVERRIDE COMMAND RECEIVED!")
                scada.emergency_shutdown()
                is_system_healthy = False
                break 
                
            stroke_counter += 1
            telemetry = generate_mock_telemetry()
            
            # Cooldown Evaluation
            if in_cooldown:
                cooldown_strokes_remaining -= 1
                sys.stdout.write(f"\rStroke {stroke_counter:04d} | [SYSTEM] Cooldown active ({cooldown_strokes_remaining} strokes left)...")
                sys.stdout.flush()
                
                with open(log_filename, mode='a', newline='') as file:
                    csv.writer(file).writerow([stroke_counter, current_spm, 0.0])
                
                if cooldown_strokes_remaining <= 0:
                    print(f"\n\n[SYSTEM] Cooldown complete. Resuming active diagnostic monitoring.")
                    in_cooldown = False
                time.sleep(0.15)
                continue 
                
            # Diagnostic Stage
            pred_class = ml_model.predict(telemetry['position'], telemetry['load'])
            sys.stdout.write(f"\rStroke {stroke_counter:04d} | ML Class: {pred_class.upper().ljust(15)}")
            sys.stdout.flush()
            
            stroke_severity = DynacardAnalytics.calculate_fluid_pound_severity(telemetry['position'], telemetry['load']) if pred_class == 'fluid_pound' else 0.0
                
            with open(log_filename, mode='a', newline='') as file:
                csv.writer(file).writerow([stroke_counter, current_spm, stroke_severity])
                
            alarm = monitor.process_new_stroke(pred_class, severity=stroke_severity)
            
            # Prescriptive Stage
            if alarm:
                if alarm['level'] == 'INFO':
                    print(f"{alarm['message']}")
                else:
                    print(f"\n\n{alarm['message']}")
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
                    
                    if scada.set_and_verify_spm(recommended_spm):
                        current_spm = recommended_spm
                        in_cooldown = True
                        cooldown_strokes_remaining = COOLDOWN_PERIOD
                        monitor.reset_history() 
                    else:
                        print(f"\n[SYSTEM FATAL] Cannot mitigate fluid pound due to SCADA failure!")
                        scada.emergency_shutdown()
                        is_system_healthy = False
            
            time.sleep(0.15) 
            
        if is_system_healthy:
            print("\n\n[SYSTEM] Simulation ended naturally.")

    except KeyboardInterrupt:
        print("\n\n[SYSTEM] Interruption received via Keyboard. Shutting down pipeline gracefully.")

if __name__ == "__main__":
    main()
