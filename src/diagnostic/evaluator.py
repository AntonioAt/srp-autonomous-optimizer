from collections import deque, Counter

class WellMonitor:
    def __init__(self, well_id, window_size=100):
        """
        Initializes the monitoring system for a specific well.
        :param well_id: String identifier for the well.
        :param window_size: Number of strokes to evaluate for chronic issues.
        """
        self.well_id = well_id
        self.window_size = window_size
        
        # Deque for O(1) append/pop operations on moving windows
        self.stroke_history = deque(maxlen=window_size)
        self.catastrophic_history = deque(maxlen=3)
        
        self.active_alarm = None 

    def process_new_stroke(self, predicted_class):
        """
        Ingests a new stroke classification from the ML model and evaluates rules.
        :param predicted_class: String ('normal', 'fluid_pound', 'gas_interference', 'rod_parted')
        :return: Dictionary containing alarm status and message, or None.
        """
        self.stroke_history.append(predicted_class)
        self.catastrophic_history.append(predicted_class)
        
        return self._evaluate_rules()

    def _evaluate_rules(self):
        """
        Evaluates the current history against physics-based failure rules.
        """
        # 1. CATASTROPHIC FAILURE RULE (Highest Priority)
        if len(self.catastrophic_history) == 3 and all(p == 'rod_parted' for p in self.catastrophic_history):
            if self.active_alarm != 'ROD_PARTED':
                self.active_alarm = 'ROD_PARTED'
                return {
                    "level": "CRITICAL",
                    "type": "ROD_PARTED",
                    "message": f"[CRITICAL] Well {self.well_id}: Sucker Rod parted! Immediate shutdown required."
                }
            return None 

        # 2. CHRONIC FAILURE RULE (Requires full window data)
        if len(self.stroke_history) == self.window_size:
            counts = Counter(self.stroke_history)
            
            pound_pct = (counts['fluid_pound'] / self.window_size) * 100.0
            gas_pct = (counts['gas_interference'] / self.window_size) * 100.0
            
            # Threshold set to 70% to prevent transient noise alarms
            if pound_pct >= 70.0:
                if self.active_alarm != 'FLUID_POUND':
                    self.active_alarm = 'FLUID_POUND'
                    return {
                        "level": "WARNING",
                        "type": "FLUID_POUND",
                        "severity_pct": pound_pct,
                        "message": f"[WARNING] Well {self.well_id}: Chronic Fluid Pound detected ({pound_pct:.1f}% occurrence)."
                    }
            elif gas_pct >= 70.0:
                if self.active_alarm != 'GAS_INTERFERENCE':
                    self.active_alarm = 'GAS_INTERFERENCE'
                    return {
                        "level": "WARNING",
                        "type": "GAS_INTERFERENCE",
                        "severity_pct": gas_pct,
                        "message": f"[WARNING] Well {self.well_id}: Chronic Gas Interference detected ({gas_pct:.1f}% occurrence)."
                    }
            else:
                if self.active_alarm in ['FLUID_POUND', 'GAS_INTERFERENCE']:
                    self.active_alarm = None
                    return {
                        "level": "INFO",
                        "type": "NORMAL",
                        "message": f"[INFO] Well {self.well_id}: Conditions normalized. Alarms cleared."
                    }
                    
        return None

    def reset_history(self):
        """Clears the history queues after a physical intervention."""
        self.stroke_history.clear()
        self.catastrophic_history.clear()
        self.active_alarm = None
