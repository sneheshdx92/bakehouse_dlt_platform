from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

@dp.table(
    name="bronze_sales_customers",
    comment="Raw customer master data ingested as-is"
)
def bronze_sales_customers():
    return (
        spark.read
            .format("delta")
            .table("samples.bakehouse.sales_customers")
            .withColumn("_ingested_at", current_timestamp())
    )
