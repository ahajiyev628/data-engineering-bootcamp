from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("week9")
         .getOrCreate())

# log_schema = "id STRING, room_id STRING, noted_date STRING, temp INT, out_in STRING, event_time STRING"

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("file:///tmp/iot-temp-input/iot-temp-input")

log_schema = df.schema

lines = spark.readStream.format("csv") \
    .schema(log_schema) \
    .option("header", "false") \
    .load("file:///tmp/iot-temp-input/iot-temp-input")

lines = (spark.readStream
         .format("csv")
         .schema(log_schema)
         .option("delimiter", "|")
         .option("header", "false")
         .load("file:///tmp/iot-temp-input"))

lines = lines.withColumn("noted_date", F.to_timestamp(F.col("noted_date"), "dd-MM-yyyy HH:mm")) \
            .withColumn("event_time", F.to_timestamp(F.col("event_time"))) \
            .withColumn("year", F.year(F.col("noted_date"))) \
            .withColumn("month", F.month(F.col("noted_date"))) \
            .withColumn("day_of_week", F.dayofweek(F.col("noted_date")))

checkpoint_dir = "file:///tmp/checkpoint"
output_path = "file:///tmp/iot-temp-output"

def write_batch_to_multiple_sinks(batch_df, batch_id):
    batch_df.cache()
    print(f"Processing batch {batch_id}")
    batch_df.show()
    batch_df.write \
        .format("csv") \
        .mode("append") \
        .option("header", "true") \
        .option("delimiter", "|") \
        .save(output_path)
    batch_df.unpersist()


streaming_query = lines.writeStream \
    .foreachBatch(write_batch_to_multiple_sinks) \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

streaming_query.awaitTermination()