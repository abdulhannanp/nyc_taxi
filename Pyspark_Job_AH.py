# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, when, avg, stddev, count, sum, expr, dayofweek, month, desc
from pyspark.sql.utils import AnalysisException
from pyspark.sql.streaming import Trigger

spark = SparkSession.builder \
    .appName("NYC_TLC_Processing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.parquet.mergeSchema", "false") \
    .config("spark.sql.autoBroadcastJoinThreshold", "50MB") \
    .getOrCreate()

# This below code to read data from parquet  

try:
    trip_data = spark.read.format("parquet").load("s3://nyc-tlc-data/trips/")
    zone_lookup = spark.read.format("parquet").load("s3://nyc-tlc-data/zones/")
except AnalysisException as e:
    print(f"Data loading error: {e}")
    spark.stop()
    exit()

# This below code is from Kafka

# Kafka Read Options
read_opts_trip_data = {
    'kafka.bootstrap.servers': '-',
    'subscribe': '-',
    'startingOffsets': 'earliest',
    'failOnDataLoss': 'true'
}

read_opts_zone_lookup = {
    'kafka.bootstrap.servers': '-',
    'subscribe': '-',
    'startingOffsets': 'earliest',
    'failOnDataLoss': 'true'
}

# Checkpoint Locations
trip_data_checkpoint = "/mnt/checkpoints/trip_data"
zone_lookup_checkpoint = "/mnt/checkpoints/zone_lookup"
output_checkpoint = "/mnt/checkpoints/output"

try:
    # Read Stream with Trigger and Checkpointing
    trip_data = (
        spark.readStream
        .format("kafka")
        .options(**read_opts_trip_data)
        .load()
        .withWatermark("timestamp", "10 minutes")  # Optional: Handles late data
    )

    zone_lookup = (
        spark.readStream
        .format("kafka")
        .options(**read_opts_zone_lookup)
        .load()
        .withWatermark("timestamp", "4 hours") 
    )

    # Write Stream with Trigger and Checkpointing
    query = (
        trip_data.writeStream
        .format("console")  # Change to "parquet", "delta", "kafka", etc.
        .option("checkpointLocation", output_checkpoint)  # Checkpointing for output
        .trigger(Trigger.ProcessingTime("10 seconds"))  # Trigger for writing
        .start()
    )

    query.awaitTermination()

except AnalysisException as e:
    print(f"Data loading error: {e}")
    spark.stop()
    exit()


trip_data = trip_data.filter((col("trip_distance") > 0) & (col("fare_amount") > 0))

trip_data = trip_data.withColumn("trip_duration", 
    (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60)
trip_data = trip_data.withColumn("trip_speed", 
    when(col("trip_duration") > 0, col("trip_distance") / (col("trip_duration") / 60)).otherwise(0))


stats = trip_data.select(avg(col("fare_amount")).alias("mean"), stddev(col("fare_amount")).alias("stddev"))
mean_val, stddev_val = stats.first()
trip_data = trip_data.withColumn("is_outlier", 
    when((col("fare_amount") > mean_val + 3 * stddev_val) | (col("fare_amount") < mean_val - 3 * stddev_val), 1).otherwise(0))

trip_data = trip_data.join(zone_lookup.hint("broadcast"), trip_data["pickup_location_id"] == zone_lookup["location_id"], "left") \
    .withColumnRenamed("zone", "pickup_zone")
trip_data = trip_data.join(zone_lookup.hint("broadcast"), trip_data["dropoff_location_id"] == zone_lookup["location_id"], "left") \
    .withColumnRenamed("zone", "dropoff_zone")

hourly_agg = trip_data.groupBy(expr("date_trunc('hour', pickup_datetime)").alias("hour")) \
    .agg(count("trip_id").alias("total_trips"), sum("fare_amount").alias("total_revenue")) \
    .repartition(10)

daily_agg = trip_data.groupBy(expr("date_trunc('day', pickup_datetime)").alias("day")) \
    .agg(count("trip_id").alias("total_trips"), sum("fare_amount").alias("total_revenue")) \
    .repartition(10)


trip_data_2024 = trip_data.filter(col("pickup_datetime").between("2024-01-01", "2024-12-31"))


top_pickup_locations = trip_data_2024.groupBy("pickup_zone") \
    .agg(sum("fare_amount").alias("total_revenue")) \
    .orderBy(desc("total_revenue")) \
    .limit(10)

top_pickup_locations.show()


trip_patterns = trip_data_2024.withColumn("day_of_week", dayofweek(col("pickup_datetime"))) \
    .groupBy(month(col("pickup_datetime")).alias("month"), 
             when(col("day_of_week").isin(1, 7), "Weekend").otherwise("Weekday").alias("day_type")) \
    .agg(count("trip_id").alias("total_trips")) \
    .orderBy("month", "day_type")

trip_patterns.show()


top_long_trips = trip_data_2024.filter(col("trip_distance") > 10) \
    .orderBy(desc("trip_distance")) \
    .limit(10)

top_long_trips.show()


try:
    trip_data.write.format("delta").mode("overwrite").save("s3://nyc-tlc-data/processed_trips/")
    hourly_agg.write.format("delta").mode("overwrite").save("s3://nyc-tlc-data/hourly_aggregations/")
    daily_agg.write.format("delta").mode("overwrite").save("s3://nyc-tlc-data/daily_aggregations/")
    print("Data processing complete!")
except Exception as e:
    print(f"Error saving data: {e}")
    spark.stop()
    exit()

spark.stop()
