class SRPPhysicsEngine:
    def __init__(self, well_id, plunger_diameter, stroke_length):
        """
        Inisialisasi spesifikasi fisik peralatan bawah permukaan sumur.
        plunger_diameter: dalam inch (misal 1.5)
        stroke_length: dalam inch (misal 120)
        """
        self.well_id = well_id
        self.D = plunger_diameter
        self.S = stroke_length
        self.constant = 0.1166 # Konstanta konversi ke Barrels Per Day (BPD)

    def calculate_pump_displacement(self, spm):
        """Menghitung kapasitas pompa teoritis (BPD) berdasarkan SPM saat ini"""
        pd = self.constant * spm * self.S * (self.D ** 2)
        return pd

    def recommend_new_spm_for_fluid_pound(self, current_spm, pound_severity_pct):
        """
        Jika terjadi Fluid Pound, berarti kapasitas pompa (PD) melebihi aliran fluida dari reservoir.
        Kita harus menurunkan SPM. 
        pound_severity_pct: Estimasi persentase ruang pompa yang kosong (misal 30% kosong).
        """
        current_pd = self.calculate_pump_displacement(current_spm)
        
        # Asumsi konservatif: Turunkan kapasitas pompa sebesar persentase keparahannya
        target_pd = current_pd * (1 - (pound_severity_pct / 100))
        
        # Hitung balik SPM yang dibutuhkan untuk mencapai Target PD
        # SPM = PD / (0.1166 * S * D^2)
        new_spm = target_pd / (self.constant * self.S * (self.D ** 2))
        
        # Aturan Keamanan Operasional: Jangan biarkan SPM turun di bawah batas minimum rotasi motor
        MIN_SPM_LIMIT = 3.0 
        recommended_spm = max(new_spm, MIN_SPM_LIMIT)
        
        return round(recommended_spm, 1)
