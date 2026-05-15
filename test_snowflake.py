from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# =========================
# 1. SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("FraudDetectionStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# 2. LECTURE KAFKA
# =========================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# =========================
# 3. SCHEMA JSON
# =========================
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("country", StringType()),
    StructField("timestamp", StringType()),
    StructField("status", StringType()),
    StructField("fraud_reasons", ArrayType(StringType()))
])

# =========================
# 4. PARSING JSON
# =========================
json_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# =========================
# 5. TIMESTAMP
# =========================
json_df = json_df.withColumn(
    "event_timestamp",
    to_timestamp(col("timestamp"))
)

# =========================
# 6. RISK SCORE
# =========================
enriched_df = json_df.withColumn(
    "risk_score",
    when(col("amount") > 10000, 0.9)
    .when(col("amount") > 5000, 0.7)
    .otherwise(0.2)
)

# =========================
# 7. FILTRE FRAUDE
# =========================
fraud_df = enriched_df.filter(col("status") == "FRAUD")

# =========================
# 8. OUTPUT KAFKA ALERTS
# =========================
fraud_query = fraud_df.selectExpr(
    "to_json(struct(*)) as value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "fraud_alerts") \
    .option("checkpointLocation", "/tmp/checkpoint_fraud") \
    .start()

# =========================
# 9. WRITE PARQUET
# =========================
parquet_query = fraud_df.writeStream \
    .format("parquet") \
    .option("path", "/tmp/fraud_parquet") \
    .option("checkpointLocation", "/tmp/checkpoint_parquet") \
    .outputMode("append") \
    .start()

# =========================
# 10. WAIT TERMINATION
# =========================
spark.streams.awaitAnyTermination()