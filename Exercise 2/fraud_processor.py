import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from kafka import KafkaProducer

# Setup 
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['JAVA_HOME'] = "C:\\Program Files\\Microsoft\\jdk-11.0.30.7-hotspot"
os.environ['PATH'] = "C:\\hadoop\\bin;" + os.environ.get('PATH', '')

#  pull the drivers 
spark = SparkSession.builder \
    .appName("FraudDetectionEngine") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.5.4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

#  At-Least-Once
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions-topic") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Deduplication
dedup_df = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["transaction_id"])

# fraud check
processed_df = dedup_df.withColumn("is_fraud", expr("amount > 5000"))

# TimescaleDB connection properties
DB_URL = "jdbc:postgresql://localhost:5433/fraud_db"
DB_PROPERTIES = {
    "user": "admin",
    "password": "admin_password",
    "driver": "org.postgresql.Driver"
}

def process_batch(batch_df, batch_id):
    batch_df.persist()
    
    # Sink 1: Write all transactions to TimescaleDB
    
    batch_df.write \
        .mode("append") \
        .jdbc(url=DB_URL, table="transactions", properties=DB_PROPERTIES)
    
    batch_df.show(5, truncate=False)

    # Sink 2: Route Frauds to secondary Kafka Alert queue
    fraud_df = batch_df.filter(col("is_fraud") == True)
    fraud_count = fraud_df.count()
    
    if fraud_count > 0:
        fraud_records = fraud_df.collect()
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        for row in fraud_records:
            alert = {
                "transaction_id": row["transaction_id"],
                "merchant": row["merchant"],
                "amount": row["amount"],
                "user_id": row["user_id"]
            }
            producer.send("fraud-alerts-topic", alert)
        producer.flush()
        producer.close()
        
    print(f"Batch {batch_id} complete. Detected {fraud_count} fraud alerts.")
    batch_df.unpersist()

# Start stream with Checkpointing for at-least-once fault-tolerance guarantees
query = processed_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "./checkpoints") \
    .start()

query.awaitTermination()
