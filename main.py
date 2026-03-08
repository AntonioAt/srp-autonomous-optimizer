import time
import random
import numpy as np
import sys
import csv
import threading
import json
import os
import datetime

# ==============================================================================
# 1. CONFIGURATION MANAGEMENT (Interactive Setup Wizard)
# ==============================================================================
def load_or_create_config(config_file="well_config.json"):
    """Loads well memory or prompts the Field Engineer for initial setup."""
    if not os.path.exists(config_file):
        print("==========================================================")
        print(" [SYSTEM SETUP] NEW WELL CONFIGURATION DETECTED")
        print("==========================================================")
        
        well_id = input("Enter Well ID (default: MINAS-102)          : ") or "MINAS-102"
        try:
            pump_dia = input("Enter Pump Diameter (inches, default: 2.25): ")
            pump_dia = float(pump_dia) if pump_dia else 2.25
            
            stroke_len = input("Enter Stroke Length (inches, default: 120): ")
            stroke_len = float(stroke_len) if stroke_len else 120.0
            
            init_spm = input("Enter Initial Motor Speed (SPM, default: 8.5): ")
            init_spm = float(init_spm) if init_spm else 8.5
        except ValueError:
            print("\n[ERROR] Invalid input! Falling back to standard default values.")
            pump_dia, stroke_len, init_spm = 2.25, 120.0, 8.5

        user_config = {
            "well_id": well_id.upper(),
            "pump_diameter_inches": pump_dia,
            "stroke_length_inches": stroke_len,
            "initial_spm": init_spm,
            "motor_minimum_spm": 4.0,           
            "max_spm_drop_step": 1.0,           
            "cooldown_period_strokes": 150,     
            "alarm_threshold_pct": 80.0         
        }
        
        with open(config_file, 'w') as f:
            json.dump(user_config, f, indent=4)
            
        print(f"\n[SYSTEM SETUP] Success! Permanent config saved to '{config_file}'.\n")
        time.sleep(1) 
        return user_config
    else:
        print(f"[SYSTEM] Booting up... Loading well memory from '{config_file}'...")
        with open(config_file, 'r') as f:
            return json.load(f)

# ==============================================================================
# 2. EDGE ANALYTICS & DIAGNOSTICS
# ==============================================================================
class DynacardAnalytics:
    @staticmethod
    def calculate_fluid_pound_severity(position, load):
        """Simulates calculus-based volumetric severity calculation."""
        return random.uniform(25.0, 40.0)

class WellMonitor:
    def __init__(self, well_id, threshold_pct, window_size=10):
        self.well_id = well_id
        self.window_size = window_size
        self.threshold_pct = threshold_pct
        self.stroke_history = []
        self.active_alarm = None

    def process_new_stroke(self, pred_class, severity=0.0):
        self.stroke_history.append((pred_class, severity))
        if len(self.stroke_history) > self.window_size:
            self.stroke_history.pop(0)

        if len(self.stroke_history) == self.window_size:
            classes = [item[0] for item in self.stroke_history]
            pound_pct = (classes.count('fluid_pound') / self.window_size) * 100.0

            if pound_pct >= self.threshold_pct:
                if self.active_alarm != 'FLUID_POUND':
                    self.active_alarm = 'FLUID_POUND'
                    avg_severity = np.mean([item[1] for item in self.stroke_history if item[0] == 'fluid_pound'])
                    return {"level": "WARNING", "type": "FLUID_POUND", "severity_pct": avg_severity}
            else:
                if self.active_alarm == 'FLUID_POUND':
                    self.active_alarm = None
                    return {"level": "INFO", "type": "NORMAL"}
        return None

    def reset_history(self):
        self.stroke_history.clear()
        self.active_alarm = None

# ==============================================================================
# 3. PHYSICS & PRESCRIPTIVE CONTROL
# ==============================================================================
class SRPPhysicsEngine:
    def __init__(self, well_id, plunger_diameter, stroke_length, min_spm):
        self.well_id = well_id
        self.constant = 0.1166
        self.D = plunger_diameter
        self.S = stroke_length
        self.MIN_SPM = min_spm

    def calculate_pump_displacement(self, spm):
        return self.constant * spm * self.S * (self.D ** 2)

    def calculate_optimal_spm(self, current_spm, severity_pct):
        current_pd = self.calculate_pump_displacement(current_spm)
        target_pd = current_pd * (1.0 - (severity_pct / 100.0))
        new_spm = target_pd / (self.constant * self.S * (self.D ** 2))
        return max(new_spm, self.MIN_SPM) 

class MockMLModel:
    def __init__(self):
        self.stroke_count = 0
        self.pound_start_stroke = random.randint(10, 50) 
        
    def predict(self, position, load):
        self.stroke_count += 1
        if self.stroke_count < self.pound_start_stroke:
            return 'normal'
        return 'fluid_pound' if random.random() < 0.60 else 'normal'

def generate_mock_telemetry():
    return {'position': np.array([0, 0.5, 1.0, 0.5, 0]), 'load': np.array([0, 1.0, 1.0, 0.2, 0])}

# ==============================================================================
# 4. IIoT TELEMETRY LAYER (MQTT PUBLISHER SIMULATOR)
# ==============================================================================
class TelemetryPublisher:
    @staticmethod
    def publish_intervention_event(well_id, severity, old_spm, new_spm, bpd_before, bpd_after, success_status):
        """Constructs and transmits the JSON payload to the centralized IIoT broker."""
        
        # Simulating gas fraction based on severity
        gas_frac = round(random.uniform(0.02, 0.08), 2)
        pump_fill = round(1.0 - (severity / 100.0) - gas_frac, 2)
        
        payload = {
            "well_id": well_id,
            "timestamp_utc": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            "event": "FLUID_POUND",
            "alarm_status": "MITIGATED" if success_status else "FAILED",
            "severity_vol_pct": round(severity / 100.0, 2),
            "diagnostics": {
                "pump_fillage": pump_fill,
                "gas_fraction": gas_frac
            },
            "control_action": {
                "parameter": "SPM",
                "previous": round(old_spm, 1),
                "new": round(new_spm, 1),
                "success": success_status
            },
            "production_estimate_bpd": {
                "before": round(bpd_before, 1),
                "after": round(bpd_after, 1)
            },
            "edge_node": "SRP_EDGE_V2.0"
        }
        
        # Convert dict to formatted JSON string
        json_payload = json.dumps(payload, indent=2)
        
        # 1. Print to Terminal (Simulating MQTT Publish)
        print("\n" + "="*60)
        print(" [IIoT TELEMETRY] PUBLISHING TO MQTT BROKER (JAKARTA SERVER)")
        print("="*60)
        print(json_payload)
        print("="*60 + "\n")
        
        # 2. Save to local outbox log
        with open("telemetry_outbox.log", "a") as f:
            f.write(json_payload + "\n,\n")

# ==============================================================================
# 5. SCADA INTERFACE & ASYNC HUMAN OVERRIDE
# ==============================================================================
class SCADAInterface:
    def __init__(self, well_id, max_retries=3):
        self.well_id = well_id
        self.max_retries = max_retries

    def set_and_verify_spm(self, target_spm):
        print(f"[SCADA] Initiating physical intervention. Target: {target_spm:.1f} SPM")
        network_success = False
        for attempt in range(1, self.max_retries + 1):
            try:
                print(f"   -> Network Write (Attempt {attempt}/{self.max_retries})...", end=" ")
                time.sleep(0.4) 
                if random.random() < 0.1: raise ConnectionError("Packet Loss")
                print("SUCCESS")
                network_success = True
                break
            except ConnectionError:
                print("FAILED")
                time.sleep(0.5) 
                
        if not network_success:
            print("[SCADA FATAL] Lost communication with VFD controller.")
            return False

        for i in range(3): 
            time.sleep(0.4)
            print(f"      ... Waiting for motor RPM to stabilize ({(i+1)*33}%)", end="\r")
        print(f"\n   -> [SCADA VERIFIED] Motor stabilized at {target_spm:.1f} SPM.")
        return True

    def emergency_shutdown(self):
        print(f"\n[SCADA CRITICAL] Sending KILL SIGNAL to {self.well_id} motor immediately!")

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
# 6. MAIN ORCHESTRATOR PIPELINE
# ==============================================================================
def main():
    config = load_or_create_config()
    
    print("==========================================================")
    print(" SRP AUTONOMOUS CONTROL EDGE-SERVER INITIALIZED")
    print("==========================================================\n")
    
    WELL_ID = config["well_id"]
    current_spm = config["initial_spm"]
    MAX_SPM_DROP_STEP = config["max_spm_drop_step"]
    COOLDOWN_PERIOD = config["cooldown_period_strokes"]
    
    monitor = WellMonitor(well_id=WELL_ID, threshold_pct=config["alarm_threshold_pct"])
    physics_engine = SRPPhysicsEngine(WELL_ID, config["pump_diameter_inches"], config["stroke_length_inches"], config["motor_minimum_spm"])
    scada = SCADAInterface(well_id=WELL_ID)
    ml_model = MockMLModel()
    
    in_cooldown = False
    cooldown_strokes_remaining = 0
    is_system_healthy = True 
    stroke_counter = 0
    
    log_filename = 'srp_operation_log.csv'
    with open(log_filename, mode='w', newline='') as file:
        csv.writer(file).writerow(['stroke_number', 'current_spm', 'pound_severity_pct'])
    
    print(f"[{WELL_ID}] Pipeline Active. Current SPM: {current_spm}")
    print(f"[{WELL_ID}] MANUAL OVERRIDE ACTIVE: Type 'STOP' and press Enter to kill the motor.\n")
    
    threading.Thread(target=listen_for_operator_override, daemon=True).start()
    
    try:
        while is_system_healthy and stroke_counter < 150: 
            
            global manual_override_triggered
            if manual_override_triggered:
                print("\n\n[CONTROL ROOM] MANUAL OVERRIDE COMMAND RECEIVED!")
                scada.emergency_shutdown()
                is_system_healthy = False
                break 
                
            stroke_counter += 1
            telemetry = generate_mock_telemetry()
            
            if in_cooldown:
                cooldown_strokes_remaining -= 1
                sys.stdout.write(f"\rStroke {stroke_counter:04d} | [SYSTEM] Cooldown active ({cooldown_strokes_remaining} strokes left)...")
                sys.stdout.flush()
                with open(log_filename, mode='a', newline='') as file:
                    csv.writer(file).writerow([stroke_counter, current_spm, 0.0])
                
                if cooldown_strokes_remaining <= 0:
                    print(f"\n\n[SYSTEM] Cooldown complete. Resuming active diagnostic monitoring.")
                    in_cooldown = False
                time.sleep(0.1) 
                continue 
                
            pred_class = ml_model.predict(telemetry['position'], telemetry['load'])
            sys.stdout.write(f"\rStroke {stroke_counter:04d} | ML Class: {pred_class.upper().ljust(15)}")
            sys.stdout.flush()
            
            stroke_severity = DynacardAnalytics.calculate_fluid_pound_severity(telemetry['position'], telemetry['load']) if pred_class == 'fluid_pound' else 0.0
                
            with open(log_filename, mode='a', newline='') as file:
                csv.writer(file).writerow([stroke_counter, current_spm, stroke_severity])
                
            alarm = monitor.process_new_stroke(pred_class, severity=stroke_severity)
            
            if alarm:
                if alarm['level'] == 'INFO':
                    print(f"\n[INFO] Well {WELL_ID}: Conditions normalized. Alarms cleared.")
                else:
                    print(f"\n\n[WARNING] Well {WELL_ID}: Chronic Fluid Pound detected. Avg Severity: {alarm['severity_pct']:.1f}%")
                    print(f"[ENGINEERING] Calculating dynamic mitigation strategy...")
                    
                    target_spm = physics_engine.calculate_optimal_spm(current_spm, alarm['severity_pct'])
                    
                    if (current_spm - target_spm) > MAX_SPM_DROP_STEP:
                        recommended_spm = current_spm - MAX_SPM_DROP_STEP
                        print(f" > Warning: Target drop is too aggressive. Applying damped step of max {MAX_SPM_DROP_STEP} SPM.")
                    else:
                        recommended_spm = target_spm
                    
                    bpd_before = physics_engine.calculate_pump_displacement(current_spm)
                    bpd_after = physics_engine.calculate_pump_displacement(recommended_spm)
                    
                    action_success = scada.set_and_verify_spm(recommended_spm)
                    
                    # --- IIoT TELEMETRY PUBLISH ---
                    TelemetryPublisher.publish_intervention_event(
                        well_id=WELL_ID, severity=alarm['severity_pct'], 
                        old_spm=current_spm, new_spm=recommended_spm, 
                        bpd_before=bpd_before, bpd_after=bpd_after, 
                        success_status=action_success
                    )
                    
                    if action_success:
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
        print("\n\n[SYSTEM] Interruption received. Shutting down pipeline gracefully.")

if __name__ == "__main__":
    main()
