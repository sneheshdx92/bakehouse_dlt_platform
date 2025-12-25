from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.table(
    name="bakehouse_dev.silver.silver_dim_customer",
    comment="Conformed customer dimension"
)
def dim_customer():
    return (
        dp.read("bronze_sales_customers")
          .select(
              col("customer_id").cast("string"),
              col("customer_name"),
              col("customer_email"),
              col("customer_created_date").cast("date")
          )
    )
