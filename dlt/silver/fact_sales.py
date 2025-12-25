from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.table(
    name="bakehouse_dev.silver.silver_fact_sales",
    comment="Sales fact table at transactionID grain (PII removed)"
)
@dp.expect_or_fail(
    "transaction_id_not_null",
    "transactionID IS NOT NULL"
)
def fact_sales():
    return (
        dp.read("bronze_sales_transactions")
          .select(
              col("transactionID").cast("string").alias("transaction_id"),
              col("customerID").cast("string").alias("customer_id"),
              col("franchiseID").cast("string").alias("franchise_id"),
              col("dateTime").cast("timestamp").alias("transaction_ts"),
              col("product"),
              col("quantity").cast("int"),
              col("unitPrice").cast("decimal(10,2)"),
              col("totalPrice").cast("decimal(10,2)"),
              col("paymentMethod")
          )
    )
