import pandas as pd

def build_sales_report(sales_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(
        sales_df,
        customers_df,
        how="left",
        on="customer_id",
        validate="many_to_one",
    )

    merged["revenue"] = merged["price"] * merged["quantity"]

    report = (
        merged
        .groupby("country", as_index=False)
        .agg(
            total_revenue=("revenue", "sum"),
            avg_order_value=("revenue", "mean"),
            order_count=("revenue", "size"),
        )
        .sort_values("total_revenue", ascending=False)
    )

    return report

