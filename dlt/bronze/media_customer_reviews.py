from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

@dp.table(
    name="bronze_media_customer_reviews",
    comment="Raw unstructured customer reviews ingested as-is"
)
def bronze_media_customer_reviews():
    return (
        spark.read
            .format("delta")
            .table("samples.bakehouse.media_customer_reviews")
            .withColumn("_ingested_at", current_timestamp())
    )
