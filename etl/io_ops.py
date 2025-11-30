from pathlib import Path
import pandas as pd

def read_sales_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

def read_customers_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

def write_parquet(df: pd.DataFrame, path: Path) -> None:
    df.to_parquet(path, index=False)

