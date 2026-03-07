import time
import random

class SCADAInterface:
    def __init__(self, well_id, max_retries=3, physical_timeout=30):
        """
        Antarmuka aman untuk komunikasi VFD lapangan.
        :param max_retries: Jumlah maksimal percobaan kirim sinyal jika jaringan putus.
        :param physical_timeout: Waktu maksimal (detik) menunggu motor mencapai target SPM.
        """
        self.well_id = well_id
        self.max_retries = max_retries
        self.physical_timeout = physical_timeout

    def _mock_network_write(self, target_spm):
        """Simulasi pengiriman paket jaringan (Modbus/MQTT) dengan peluang gagal."""
        if random.random() < 0.2: # 20% peluang jaringan putus/latency
            raise ConnectionError("Timeout: VFD tidak merespons (Packet Loss).")
        return True

    def _mock_read_actual_spm(self, target_spm, elapsed_time):
        """Simulasi inersia fisik motor. Butuh waktu untuk mencapai target."""
        # Motor mensimulasikan ramp-down perlahan
        if elapsed_time < 5.0:
            return target_spm + 1.5 # Masih jauh dari target
        elif elapsed_time < 10.0:
            return target_spm + 0.5 # Mendekati target
        else:
            return target_spm       # Target tercapai

    def set_and_verify_spm(self, target_spm):
        """
        Fungsi eksekusi inti dengan Two-Way Handshake dan Physical Verification.
        Mengembalikan True jika sukses secara mekanik, False jika gagal total.
        """
        print(f"\n[SCADA] Memulai intervensi untuk {self.well_id}. Target: {target_spm} SPM")
        
        # TAHAP 1: NETWORK WRITE & RETRY LOGIC
        network_success = False
        for attempt in range(1, self.max_retries + 1):
            try:
                print(f" > Network Write (Attempt {attempt}/{self.max_retries})...", end=" ")
                self._mock_network_write(target_spm)
                print("SUCCESS (Register Updated)")
                network_success = True
                break
            except ConnectionError as e:
                print(f"FAILED ({e})")
                time.sleep(2) # Backoff sebelum retry
                
        if not network_success:
            print("[SCADA FATAL] Gagal menghubungi VFD. Aborting intervention.")
            return False

        # TAHAP 2: PHYSICAL VERIFICATION (Ramp-down Polling)
        print(" > Memverifikasi respons mekanis motor...")
        start_time = time.time()
        
        while (time.time() - start_time) < self.physical_timeout:
            elapsed = time.time() - start_time
            actual_spm = self._mock_read_actual_spm(target_spm, elapsed)
            
            # Toleransi margin error pembacaan sensor (misal +/- 0.1 SPM)
            if abs(actual_spm - target_spm) <= 0.1:
                print(f" > [SCADA VERIFIED] Motor stabil pada {actual_spm} SPM. Waktu: {elapsed:.1f} detik.")
                return True
                
            print(f"   ... Motor RPM masih transisi (Actual: {actual_spm:.1f} SPM)", end="\r")
            time.sleep(2) # Polling interval
            
        print(f"\n[SCADA FATAL] Timeout! Motor tidak mencapai target dalam {self.physical_timeout} detik.")
        return False
