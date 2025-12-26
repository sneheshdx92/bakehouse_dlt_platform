from pyspark import pipelines as dp

@dp.view(name="source")
def source():
 return spark.read.table("bakehouse_dev.silver.silver_dim_customer")

dp.create_streaming_table("bakehouse_dev.silver.silver_dim_customer_scd2")

# 2. Apply snapshot-based CDC
dp.create_auto_cdc_from_snapshot_flow(
 target="bakehouse_dev.silver.silver_dim_customer_scd2",
 source="source",
 keys=["customer_id"],
 stored_as_scd_type=2
)





