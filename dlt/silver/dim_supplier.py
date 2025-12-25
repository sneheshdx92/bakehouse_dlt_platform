from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.table(
    name="bakehouse_dev.silver.silver_dim_supplier",
    comment="Conformed supplier dimension"
)
def dim_supplier():
    return (
        dp.read("bronze_sales_suppliers")
          .select(
              col("supplierID").cast("string").alias("supplier_id"),
              col("name").alias("supplier_name"),
              col("ingredient"),
              col("continent"),
              col("city"),
              col("district"),
              col("size"),
              col("longitude"),
              col("latitude"),
              col("approved").cast("boolean")
          )
    )
