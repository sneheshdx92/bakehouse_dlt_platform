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
              col("supplier_id").cast("string"),
              col("supplier_name"),
              col("supplier_type"),
              col("country")
          )
    )
