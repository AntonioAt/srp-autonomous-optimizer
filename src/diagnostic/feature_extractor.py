import numpy as np
from scipy.signal import savgol_filter

class DynacardAnalytics:
    @staticmethod
    def calculate_fluid_pound_severity(position, load):
        """
        Analytically determines the volumetric severity of a fluid pound.
        Returns the percentage of empty pump volume (0.0 to 100.0).
        """
        # 1. Signal Smoothing
        load_smooth = savgol_filter(load, window_length=11, polyorder=3)

        # 2. Isolate Downstroke
        top_idx = np.argmax(position)
        pos_down = position[top_idx:]
        load_down = load_smooth[top_idx:]

        # Prevent calculation if data is malformed (e.g., extremely short downstroke)
        if len(pos_down) < 5:
            return 0.0

        # 3. First Derivative (Load vs Time-Index)
        d1 = np.gradient(load_down)
        
        # 4. Find Inflection Point (Max negative slope)
        impact_idx = np.argmin(d1)
        x_impact = pos_down[impact_idx]

        # 5. Calculate Volumetric Severity
        severity_vol = (1.0 - x_impact) * 100.0
        
        # Bound the result between 0 and 100
        return max(0.0, min(100.0, severity_vol))
