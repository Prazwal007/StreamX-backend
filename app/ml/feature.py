import numpy as np

def extract_features(metrics: dict) -> np.ndarray:
    return np.array([
        metrics["avg_speed"] / 10_000,
        metrics["speed_variance"] / 5_000,
        metrics["retries"] / 5,
        metrics["connections"] / 8,
        metrics["chunk_kb"] / 1024,
        metrics["rtt_ms"] / 1000,
    ], dtype=np.float32)
