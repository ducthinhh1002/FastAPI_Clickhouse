import argparse
import os
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np


def generate_table(num_rows: int, num_cols: int) -> pa.Table:
    arrays = []
    field_names = []
    for i in range(num_cols):
        field_names.append(f"col{i}")
        arrays.append(pa.array(np.random.randint(0, 1_000_000, size=num_rows)))
    return pa.Table.from_arrays(arrays, names=field_names)


def main():
    parser = argparse.ArgumentParser(description="Generate Parquet file with random data")
    parser.add_argument("--rows", type=int, default=10_000_000, help="Number of rows")
    parser.add_argument("--cols", type=int, default=10, help="Number of columns")
    parser.add_argument("--output", default="data.parquet", help="Output parquet file")
    args = parser.parse_args()

    table = generate_table(args.rows, args.cols)
    pq.write_table(table, args.output)
    print(f"Generated {args.output} with {args.rows} rows and {args.cols} columns")


if __name__ == "__main__":
    main()
