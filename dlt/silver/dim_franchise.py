from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.table(
    name="bakehouse_dev.silver.silver_dim_franchise",
    comment="Conformed franchise (store) dimension"
)
def dim_franchise():
    return (
        dp.read("bronze_sales_franchises")
          .select(
              col("franchise_id").cast("string"),
              col("franchise_name"),
              col("city"),
              col("state"),
              col("country"),
              col("opened_date").cast("date")
          )
    )
