from collections import deque, Counter
import numpy as np

class WellMonitor:
    def __init__(self, well_id, window_size=100):
        self.well_id = well_id
        self.window_size = window_size
        
        # Queue now stores tuples: (predicted_class, severity_value)
        self.stroke_history = deque(maxlen=window_size)
        self.catastrophic_history = deque(maxlen=3)
        self.active_alarm = None 

    def process_new_stroke(self, predicted_class, severity=0.0):
        """
        Ingests stroke classification and its calculated physical severity.
        """
        self.stroke_history.append((predicted_class, severity))
        self.catastrophic_history.append(predicted_class)
        
        return self._evaluate_rules()

    def _evaluate_rules(self):
        # 1. CATASTROPHIC FAILURE RULE
        if len(self.catastrophic_history) == 3 and all(p == 'rod_parted' for p in self.catastrophic_history):
            if self.active_alarm != 'ROD_PARTED':
                self.active_alarm = 'ROD_PARTED'
                return {
                    "level": "CRITICAL",
                    "type": "ROD_PARTED",
                    "message": f"[CRITICAL] Well {self.well_id}: Sucker Rod parted! Immediate shutdown required."
                }
            return None 

        # 2. CHRONIC FAILURE RULE
        if len(self.stroke_history) == self.window_size:
            # Extract just the classes for counting
            classes = [item[0] for item in self.stroke_history]
            counts = Counter(classes)
            
            pound_pct = (counts['fluid_pound'] / self.window_size) * 100.0
            gas_pct = (counts['gas_interference'] / self.window_size) * 100.0
            
            if pound_pct >= 70.0:
                if self.active_alarm != 'FLUID_POUND':
                    self.active_alarm = 'FLUID_POUND'
                    
                    # Calculate AVERAGE severity across all fluid pound strokes in the window
                    pound_severities = [item[1] for item in self.stroke_history if item[0] == 'fluid_pound']
                    avg_severity = np.mean(pound_severities) if pound_severities else 0.0

                    return {
                        "level": "WARNING",
                        "type": "FLUID_POUND",
                        "severity_pct": avg_severity, # Replaced frequency with actual volume severity
                        "message": f"[WARNING] Well {self.well_id}: Chronic Fluid Pound. Avg Empty Volume: {avg_severity:.1f}%."
                    }
            # ... (Gas interference logic remains similar, omitted for brevity)
            else:
                if self.active_alarm in ['FLUID_POUND', 'GAS_INTERFERENCE']:
                    self.active_alarm = None
                    return {
                        "level": "INFO",
                        "type": "NORMAL",
                        "message": f"[INFO] Well {self.well_id}: Conditions normalized."
                    }
        return None

    def reset_history(self):
        self.stroke_history.clear()
        self.catastrophic_history.clear()
        self.active_alarm = None
