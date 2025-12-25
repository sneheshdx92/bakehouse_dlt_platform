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
              col("franchiseID").cast("string").alias("franchise_id"),
              col("name").alias("franchise_name"),
              col("city"),
              col("district"),
              col("zipcode"),
              col("country"),
              col("size"),
              col("longitude"),
              col("latitude"),
              col("supplierID").cast("string").alias("supplier_id")
          )
    )
