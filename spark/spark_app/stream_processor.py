from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import *

# ======== 1. Tạo Spark Session ========
spark = SparkSession.builder \
    .appName("RealtimeUserEventIngestion") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ======== 2. Khai báo schema dữ liệu Kafka ========
event_schema = StructType([
    StructField("user_id", IntegerType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("action", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False),
    StructField("device_id", StringType(), nullable=True),
    StructField("device_type", StringType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("user_segment", StringType(), nullable=True),
    StructField("ip_address", StringType(), nullable=True)
])

# ======== 3. Đọc stream từ Kafka topic ========
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

# ======== 4. Parse JSON và chuẩn hóa dữ liệu ========
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), event_schema).alias("data")) \
    .filter(col("data").isNotNull()) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("timestamp"))

# ======== 5. Hàm ghi batch vào PostgreSQL ========
def write_to_postgres(batch_df, batch_id):
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/realtime_db") \
            .option("dbtable", "user_events") \
            .option("user", "user") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Error writing batch {batch_id}: {e}")
        # Handle error (e.g., log it, send alert, etc.)
        raise e
    else:
        print(f"Batch {batch_id} written successfully to PostgreSQL.")
    
    finally:
        batch_df.unpersist()
        print(f"Batch {batch_id} unpersisted.")
        
# ======== 6. Ghi dữ liệu streaming vào PostgreSQL ========
query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/opt/spark/spark-checkpoints/user_events") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
