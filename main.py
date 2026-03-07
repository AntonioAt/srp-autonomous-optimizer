import time
from src.diagnostic.evaluator import WellMonitor
from src.prescriptive.physics_solver import SRPPhysicsEngine

def execute_scada_command(well_id, command_type, value):
    """
    Mock function to simulate sending commands to the field VFD via SCADA/Modbus.
    """
    print(f"\n---> [SCADA TRANSMISSION] Sending {command_type} = {value} to {well_id} VFD...")
    time.sleep(1) # Simulate network latency
    print(f"---> [SCADA ACKNOWLEDGE] Command executed successfully.\n")

def main():
    print("Initializing SRP Autonomous Control Pipeline...\n")
    
    # 1. System Initialization
    WELL_ID = "ALPHA-01"
    current_spm = 8.5
    
    monitor = WellMonitor(well_id=WELL_ID, window_size=10) # Small window for simulation
    physics_engine = SRPPhysicsEngine(well_id=WELL_ID, plunger_diameter=1.5, stroke_length=120)
    
    # Control Dynamics Variables
    in_cooldown = False
    cooldown_strokes_remaining = 0
    COOLDOWN_PERIOD = 15 # Wait for 15 strokes before re-evaluating after an SPM change
    
    # 2. Simulated Real-Time Telemetry Stream (Predictions from ML Model)
    # Scenario: Normal -> Chronic Fluid Pound -> Intervention -> Recovery
    simulated_stream = (
        ['normal'] * 8 + 
        ['fluid_pound'] * 12 + 
        ['normal'] * 20 
    )
    
    print(f"[{WELL_ID}] Pipeline Active. Current SPM: {current_spm}\n")
    
    # 3. The Continuous Execution Loop
    for stroke_num, pred_class in enumerate(simulated_stream, 1):
        print(f"Stroke {stroke_num:03d} | ML Classification: {pred_class.upper()}", end="\r")
        
        # Check if well is stabilizing (Dead Time / Cooldown)
        if in_cooldown:
            cooldown_strokes_remaining -= 1
            if cooldown_strokes_remaining <= 0:
                print(f"\n[SYSTEM] Cooldown period ended. Resuming active monitoring for {WELL_ID}.")
                in_cooldown = False
            time.sleep(0.1)
            continue # Skip diagnostic evaluation while fluid levels equalize
            
        # Stage 1: Evaluate Rules
        alarm = monitor.process_new_stroke(pred_class)
        
        if alarm:
            print(f"\n\n{alarm['message']}")
            
            if alarm['type'] == 'ROD_PARTED':
                execute_scada_command(WELL_ID, 'MOTOR_POWER', 'OFF')
                print("[SYSTEM] Catastrophic failure handled. Halting operations.")
                break # Stop the loop
                
            elif alarm['type'] == 'FLUID_POUND':
                # Stage 2: Prescriptive Action
                severity = alarm['severity_pct']
                print(f"[ENGINEERING] Calculating mitigation strategy...")
                
                recommended_spm = physics_engine.calculate_optimal_spm(current_spm, severity)
                
                print(f" > Current Capacity ({current_spm} SPM) : {physics_engine.calculate_pump_displacement(current_spm):.1f} BPD")
                print(f" > Target Capacity  ({recommended_spm} SPM) : {physics_engine.calculate_pump_displacement(recommended_spm):.1f} BPD")
                
                # Execute Action
                execute_scada_command(WELL_ID, 'SET_SPM', recommended_spm)
                
                # Update State
                current_spm = recommended_spm
                in_cooldown = True
                cooldown_strokes_remaining = COOLDOWN_PERIOD
                
                # CRITICAL: Reset the diagnostic history so we don't double-trigger on old data
                monitor.reset_history() 
                print(f"[SYSTEM] Entering {COOLDOWN_PERIOD}-stroke cooldown period for annular fluid equilibrium...")
        
        time.sleep(0.1) # Simulate time between strokes

if __name__ == "__main__":
    main()
