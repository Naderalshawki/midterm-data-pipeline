import argparse
import csv
from pathlib import Path


def create_sample(input_path: str, output_path: str, rows_count: int):
    inp = Path(input_path)
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    print(f"[*] Extracting {rows_count:,} sample rows from {inp.name} -> {out.name}...")
    with open(inp, mode="r", encoding="utf-8", errors="ignore") as f_in, \
         open(out, mode="w", encoding="utf-8", newline="") as f_out:
        
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        
        header = next(reader)
        writer.writerow(header)
        
        count = 0
        for row in reader:
            writer.writerow(row)
            count += 1
            if count >= rows_count:
                break

    print(f"[+] Successfully generated sample: {out} ({count:,} rows, {round(out.stat().st_size / (1024*1024), 2)} MB)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create small reproducible sample from large CSV")
    parser.add_argument("--input", default="data/orders_huge_mixed_quality.csv", help="Input large CSV")
    parser.add_argument("--output", default="data/orders_small_sample.csv", help="Output sample CSV")
    parser.add_argument("--rows", type=int, default=100000, help="Number of rows")
    args = parser.parse_args()

    create_sample(args.input, args.output, args.rows)