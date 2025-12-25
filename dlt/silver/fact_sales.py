from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.table(
    name="bakehouse_dev.silver.silver_fact_sales",
    comment="Sales fact table at transactionID grain"
)
@dp.expect_or_fail(
    "unique_transaction",
    "transactionID IS NOT NULL"
)
def fact_sales():
    return (
        dp.read("bronze_sales_transactions")
          .select(
              col("transactionID"),
              col("customer_id"),
              col("franchise_id"),
              col("supplier_id"),
              col("transaction_timestamp").cast("timestamp"),
              col("quantity").cast("int"),
              col("unit_price").cast("decimal(10,2)")
          )
    )
