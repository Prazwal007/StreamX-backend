import csv
import time

class MetricsLogger:
    def __init__(self):
        self.metrics = []

    def log(self, downloaded, total, chunk_size, speed_kbps):
        now = time.time()
        self.metrics.append({
            "time": now,
            "downloaded": downloaded,
            "total": total,
            "chunk_size": chunk_size,
            "speed_kbps": speed_kbps
        })

    def save_csv(self, filename):
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(
                f, fieldnames=["time", "downloaded", "total", "chunk_size", "speed_kbps"]
            )
            writer.writeheader()
            writer.writerows(self.metrics)
