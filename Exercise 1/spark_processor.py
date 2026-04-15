import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# initiating the spark session
os.environ['HADOOP_HOME'] = "C:\\hadoop"
os.environ['JAVA_HOME'] = "C:\\Program Files\\Microsoft\\jdk-11.0.30.7-hotspot"
os.environ['PATH'] = "C:\\hadoop\\bin;" + os.environ.get('PATH', '')

spark = SparkSession.builder \
    .appName("KafkaTransformer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# reading ythe data 
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "input-topic") \
    .load()

# Parse JSON into columns
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# transform
transformed_df = parsed_df \
    .withColumn("amount_with_tax", col("amount") * 1.1) \
    .withColumn("processed_by", lit("spark-streaming"))

# sink and batch processing
def process_batch(batch_df, batch_id):
    batch_df.persist()

    
    print(f"--- BATCH {batch_id} Transformed Data ---")
    batch_df.show(truncate=False)
    
    # 2. Count messages per batch
    batch_count = batch_df.count()
    print(f"Total processed in Batch {batch_id} = {batch_count} messages\n")
    
   
    # Save transformed data to Sink (local JSON)
    batch_df.write.mode("append").json("output_sink")
    
    # Publish count to another Kafka topic
     # this is to avoid the nulls processing going into otehr topic
    if batch_count > 0:
        import json
        from kafka import KafkaProducer
        
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        payload = {"batch_id": batch_id, "message_count": batch_count}
        producer.send("count-topic", payload)
        producer.flush()
        producer.close()
            
    batch_df.unpersist()

# Start streaming
query = transformed_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
