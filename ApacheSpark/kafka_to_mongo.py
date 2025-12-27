from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json
import time

spark = (
    SparkSession.builder
    .appName("KafkaToMongo")
    .getOrCreate()
)

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("visitorid", StringType(), True),
    StructField("event", StringType(), True),
    StructField("itemid", StringType(), True),
    StructField("transactionid", StringType(), True),
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "ingestion-topic")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING)")
      .withColumn("json", from_json("value", schema))
      .select("json.*")
)

def write_to_mongo(batch_df, batch_id):
    batch_df.write \
        .format("mongodb") \
        .mode("append") \
        .option(
            "spark.mongodb.connection.uri",
            "mongodb://root:example@mongo-dev:27017/docstreaming.events?authSource=admin"
        ) \
        .save()

query = (
    parsed.writeStream
    .foreachBatch(write_to_mongo)
    .option("checkpointLocation", "/tmp/checkpoints/mongo")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()

# print("=== Streaming terminated ===")
# print("isActive:", query.isActive)
# print("status:", query.status)
# print("lastProgress:", query.lastProgress)
# print("exception:", query.exception())

# print("Streaming finished, keeping driver alive...")
# while True:
#     time.sleep(3600)