from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

@dp.table(
    name="bronze_sales_suppliers",
    comment="Raw supplier reference data ingested as-is"
)
def bronze_sales_suppliers():
    return (
        spark.read
            .format("delta")
            .table("samples.bakehouse.sales_suppliers")
            .withColumn("_ingested_at", current_timestamp())
    )
