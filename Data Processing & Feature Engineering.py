# Databricks notebook source
import logging
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, current_timestamp, countDistinct, sum, avg, max, datediff, current_date, round

# ==========================================
# Logging Setup
# ==========================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==========================================
# Configurations for Paths
# ==========================================
config = {
    "bronze_table_path": "/dbfs/tmp/marketplace_bronze/",
    "silver_table_path": "/dbfs/tmp/marketplace_silver/",
    "gold_table_path": "/dbfs/tmp/marketplace_gold/",
    "checkpoint_path_bronze": "/dbfs/tmp/marketplace_checkpoint_bronze/",
    "checkpoint_path_silver": "/dbfs/tmp/marketplace_checkpoint_silver/",
}

# ==========================================
# Bronze Layer: Raw Data Ingestion
# ==========================================
def ingest_bronze_layer():
    logger.info("Starting Bronze Layer ingestion")

    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

    streaming_df = (
        spark.readStream
        .format("rate")  # Simulate a streaming data source
        .option("rowsPerSecond", 10)  # Control the data ingestion rate
        .load()
        .selectExpr("value as transaction_id")
        .withColumn("user_id", (col("transaction_id") % 100 + 1))
        .withColumn("product_id", (col("transaction_id") % 50 + 1))
        .withColumn("quantity", (col("transaction_id") % 5 + 1))
        .withColumn("price", round(col("transaction_id") * 0.1 + 5.0, 2))  # Round price to 2 decimals
        .withColumn("timestamp", current_timestamp())  # Add real-time timestamp
    )

    streaming_query = (
        streaming_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config["checkpoint_path_bronze"])
        .start(config["bronze_table_path"])
    )
    streaming_query.awaitTermination(timeout=30)  # Stop streaming after 30 seconds
    logger.info("Bronze Layer ingestion completed")

    # Display top 10 records of Bronze Layer
    logger.info("Displaying top 10 records from Bronze Layer:")
    bronze_df = spark.read.format("delta").load(config["bronze_table_path"])
    bronze_df.show(10, truncate=False)

# ==========================================
# Silver Layer: Cleaning and Validation
# ==========================================
def clean_and_validate_data(df):
    logger.info("Starting Silver Layer cleaning and validation")
    cleaned_df = (
        df.select(
            col("transaction_id").cast("int"),
            col("user_id").cast("int"),
            col("product_id").cast("int"),
            col("quantity").cast("int"),
            round(col("price"), 2).alias("price"),  # Ensure price is rounded
            col("timestamp").cast("timestamp")
        )
        .filter(
            col("transaction_id").isNotNull() &
            col("user_id").isNotNull() &
            col("product_id").isNotNull() &
            col("quantity").isNotNull() &
            col("price").isNotNull() &
            col("timestamp").isNotNull()
        )
        .filter((col("quantity") > 0) & (col("price") > 0))  # Business logic validation
        .dropDuplicates(["transaction_id"])  # Deduplication
    )
    logger.info("Silver Layer cleaning and validation completed")
    return cleaned_df

def process_silver_layer():
    bronze_df = spark.read.format("delta").load(config["bronze_table_path"])
    silver_df = clean_and_validate_data(bronze_df)
    silver_df.write.format("delta").mode("overwrite").save(config["silver_table_path"])
    logger.info("Silver Layer processing completed")

# ==========================================
# Gold Layer: Feature Engineering
# ==========================================
def generate_gold_features(silver_df):
    logger.info("Starting Gold Layer feature calculations")
    gold_df = (
        silver_df
        .groupBy("user_id")
        .agg(
            countDistinct("transaction_id").alias("purchase_frequency"),
            round(sum("price"), 2).alias("total_spend"),
            round(avg("price"), 2).alias("avg_transaction_value"),
            max("timestamp").alias("last_purchase_date")
        )
        .withColumn(
            "recency_days",
            datediff(current_date(), col("last_purchase_date"))
        )
    )
    return gold_df

def process_gold_layer():
    silver_df = spark.read.format("delta").load(config["silver_table_path"])
    gold_df = generate_gold_features(silver_df).repartition(10).cache()

    # Write Gold Layer with Partitioning and Overwrite Schema
    gold_df.write.format("delta") \
        .partitionBy("user_id") \
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .save(config["gold_table_path"])

    gold_df.unpersist()
    logger.info("Gold Layer processing completed")

# ==========================================
# Delta Table Optimization
# ==========================================
def optimize_table(table_path, zorder_column=None):
    logger.info(f"Optimizing Delta table at {table_path}")
    if zorder_column:
        # Perform Z-Ordering on a non-partition column
        spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zorder_column})")
    else:
        # Perform general optimization without Z-Ordering
        spark.sql(f"OPTIMIZE delta.`{table_path}`")
    logger.info(f"Optimization completed for {table_path}")


# ==========================================
# Metrics and Verification
# ==========================================
def generate_data_quality_metrics(df, layer_name):
    logger.info(f"Data Quality Metrics for {layer_name} Layer:")
    if "transaction_id" in df.columns:
        logger.info(f"Null Records in transaction_id: {df.filter(col('transaction_id').isNull()).count()}")
        logger.info(f"Duplicate Records: {df.count() - df.dropDuplicates(['transaction_id']).count()}")
    logger.info(f"Total Records: {df.count()}")
    logger.info(f"Columns: {df.columns}")

def verify_layers():
    for layer, path in [("Bronze", config["bronze_table_path"]),
                        ("Silver", config["silver_table_path"]),
                        ("Gold", config["gold_table_path"])]:
        df = spark.read.format("delta").load(path)
        generate_data_quality_metrics(df, layer)
        df.show(10, truncate=False)

# ==========================================
# Orchestrating the Pipeline
# ==========================================
def run_pipeline():
    ingest_bronze_layer()
    # Optimize Bronze Layer with Z-Ordering on transaction_id
    optimize_table(config["bronze_table_path"], zorder_column="transaction_id")
    process_silver_layer()
    process_gold_layer()
    # Optimize Gold Layer with Z-Ordering on total_spend
    optimize_table(config["gold_table_path"], zorder_column="total_spend")
    verify_layers()


# Execute the Pipeline
run_pipeline()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.`/dbfs/tmp/marketplace_bronze/`
# MAGIC LIMIT 500;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.`/dbfs/tmp/marketplace_silver/`
# MAGIC WHERE quantity > 0 AND price > 0
# MAGIC LIMIT 500;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.`/dbfs/tmp/marketplace_gold/`
# MAGIC ORDER BY total_spend DESC
# MAGIC LIMIT 500;
