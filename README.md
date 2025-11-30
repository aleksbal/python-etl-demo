# python-etl-demo

Compact “mini-project / cheat sheet” you can drop into a folder and play with.
It touches several points: ETL structure, Pandas, PySpark, venv, batch vs streaming, pytest, env vars.

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
