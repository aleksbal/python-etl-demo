# main.py
import os
from pathlib import Path

from etl.io_ops import read_sales_csv, read_customers_csv, write_parquet
from etl.transform import build_sales_report
from etl import spark_batch, spark_streaming


def main():
    # --- environment variables with defaults ---
    data_dir = Path(os.environ.get("DATA_DIR", "data"))
    output_dir = Path(os.environ.get("OUTPUT_DIR", "output"))
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Using DATA_DIR={data_dir}, OUTPUT_DIR={output_dir}")

    # -------- Batch ETL with Pandas --------
    print("Running Pandas batch ETL...")

    sales_df = read_sales_csv(data_dir / "sales.csv")
    customers_df = read_customers_csv(data_dir / "customers.csv")

    report_df = build_sales_report(sales_df, customers_df)
    print(report_df)

    # write result as Parquet
    write_parquet(report_df, output_dir / "sales_report.parquet")

    # -------- PySpark batch demo --------
    # (comment out if you don't have Spark configured)
    print("\nRunning PySpark batch demo...")
    spark_batch.run_spark_batch_example(str(data_dir / "sales.csv"))

    # -------- PySpark streaming demo (conceptual) --------
    # This is just a "cheat sheet" example, not meant to be run here.
    print("\nTo see streaming example, look at etl/spark_streaming.py.")


if __name__ == "__main__":
    main()
