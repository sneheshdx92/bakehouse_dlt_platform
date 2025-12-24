from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

@dp.table(
    name="bronze_sales_franchises",
    comment="Raw franchise (store) reference data ingested as-is"
)
def bronze_sales_franchises():
    return (
        spark.read
            .format("delta")
            .table("samples.bakehouse.sales_franchises")
            .withColumn("_ingested_at", current_timestamp())
    )
