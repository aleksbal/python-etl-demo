# Python ETL Demo (Cheat Sheet)

```
python-etl-demo/
├─ main.py
├─ etl/
│  ├─ __init__.py
│  ├─ io_ops.py          # Pandas I/O + env vars
│  ├─ transform.py       # Pandas transforms (merge, groupby)
│  ├─ spark_batch.py     # PySpark batch example
│  └─ spark_streaming.py # PySpark streaming example
├─ tests/
│  └─ test_transform.py  # pytest unit tests
├─ requirements.txt
└─ README.md
```

This mini-project demonstrates:

- Project structure for ETL (`main.py`, `etl/` modules, `tests/`)
- Pandas basics: `read_csv()`, `merge()`, `groupby()`, `to_parquet()`
- PySpark basics: DataFrame, lazy execution, simple transformations
- Virtual environments with `venv` and dependency installation with `pip`
- Difference between batch and streaming (with Spark Structured Streaming example)
- Unit testing with `pytest`
- Using environment variables via `os.environ`

## Setup

```bash
python -m venv .venv
source .venv/bin/activate       # or .\.venv\Scripts\Activate.ps1 on Windows
pip install -r requirements.txt
```

## Running the batch ETL

This project includes a small script to generate advanced **synthetic demo data** for Pandas and Spark benchmarks.  
The generated CSV files are written to `data/` and are **not committed to Git** (see `.gitignore`).

### Generate synthetic datasets

From the project root:

```bash
# activate venv
source .venv/bin/activate

# install dependencies (once)
pip install -r requirements.txt

# generate synthetic data into ./data
python scripts/generate_synthetic_data.py


```bash
export DATA_DIR=./data
export OUTPUT_DIR=./output
python main.py
```

## Running tests
```bash
pytest
```

