import pandas as pd
from etl.transform import build_sales_report

def test_build_sales_report_basic():
    sales_df = pd.DataFrame(
        [
            {"customer_id": 1, "price": 10.0, "quantity": 2},
            {"customer_id": 2, "price": 20.0, "quantity": 1},
            {"customer_id": 1, "price": 5.0, "quantity": 4},
        ]
    )

    customers_df = pd.DataFrame(
        [
            {"customer_id": 1, "country": "DE"},
            {"customer_id": 2, "country": "AT"},
        ]
    )

    report = build_sales_report(sales_df, customers_df)

    assert list(report["country"]) == ["DE", "AT"]

    de_row = report[report["country"] == "DE"].iloc[0]
    at_row = report[report["country"] == "AT"].iloc[0]

    assert de_row["total_revenue"] == 40.0
    assert at_row["total_revenue"] == 20.0

    assert de_row["order_count"] == 2
    assert at_row["order_count"] == 1

