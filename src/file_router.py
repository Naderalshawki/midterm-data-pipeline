"""
File Router Component (Section 6.2)
Automatically decides processing engine based on file size threshold.
"""
from pathlib import Path
from config.settings import SMALL_FILE_THRESHOLD_MB


def route_file(file_path: str) -> str:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    file_size_bytes = path.stat().st_size
    file_size_mb = file_size_bytes / (1024 * 1024)

    print("=" * 70)
    print("HYBRID DATA PIPELINE ROUTER (Section 6.2)")
    print("=" * 70)
    print(f"File Name     : {path.name}")
    print(f"File Size     : {file_size_mb:.2f} MB")
    print(f"Threshold     : {SMALL_FILE_THRESHOLD_MB} MB")

    if file_size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = f"File size <= threshold ({SMALL_FILE_THRESHOLD_MB} MB). Executing Python Batch Streaming."
    else:
        engine = "pyspark"
        reason = f"File size > threshold ({SMALL_FILE_THRESHOLD_MB} MB). Executing Parallel Apache Spark."

    print(f"Engine Chosen : {engine}")
    print(f"Reason        : {reason}")
    print("=" * 70)

    return engine


if __name__ == "__main__":
    import sys
    test_file = sys.argv[1] if len(sys.argv) > 1 else "data/orders_small_sample.csv"
    route_file(test_file)