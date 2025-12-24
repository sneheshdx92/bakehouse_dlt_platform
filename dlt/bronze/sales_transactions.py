from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

@dp.table(
    name="bronze_sales_transactions",
    comment="Raw sales transactions ingested as-is from bakehouse dataset"
)
def bronze_sales_transactions():
    return (
        spark.read
            .format("delta")
            .table("samples.bakehouse.sales_transactions")
            .withColumn("_ingested_at", current_timestamp())
    )
