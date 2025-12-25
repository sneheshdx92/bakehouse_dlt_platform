from pyspark import pipelines as dp
from pyspark.sql.functions import col, concat_ws

@dp.table(
    name="bakehouse_dev.silver.silver_dim_customer",
    comment="Conformed customer dimension (PII removed)"
)
def dim_customer():
    return (
        dp.read("bronze_sales_customers")
          .select(
              col("customerID").cast("string").alias("customer_id"),
              concat_ws(" ", col("first_name"), col("last_name")).alias("customer_name"),
              col("gender"),
              col("city"),
              col("state"),
              col("country"),
              col("continent"),
              col("postal_zip_code")
          )
    )
