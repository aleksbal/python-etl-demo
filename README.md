# python-etl-demo

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
