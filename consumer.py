from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import SparkSession
from configs import kafka_config, my_name
import json
import os

# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Створення SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())

# Читання потоку даних із Kafka
# Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
# maxOffsetsPerTrigger - будемо читати 5 записів за 1 тригер.
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("subscribe", f"{my_name}_building_sensors") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5") \
    .load()

# Визначення схеми для JSON,
# оскільки Kafka має структуру ключ-значення, а значення має формат JSON.
json_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("temp", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
])

# Маніпуляції з даними
clean_df = df.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized", "*") \
    .drop('key', 'value') \
    .withColumnRenamed("key_deserialized", "key") \
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
    .withColumn("timestamp", from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp")) \
    .withColumn("temp", col("value_json.temp")) \
    .withColumn("humidity", col("value_json.humidity")) \
    .drop("value_json", "value_deserialized")

# Додавання watermark та обчислення середньої температури та вологості за допомогою sliding window
windowed_avg_df = clean_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds")) \
    .agg(avg("temp").alias("avg_temp"), avg("humidity").alias("avg_humidity"))

# Читання файлу alerts_conditions.csv
alerts_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("humidity_min", IntegerType(), True),
    StructField("humidity_max", IntegerType(), True),
    StructField("temperature_min", IntegerType(), True),
    StructField("temperature_max", IntegerType(), True),
    StructField("code", IntegerType(), True),
    StructField("message", StringType(), True)
])

alerts_df = spark.read.option("header", "true").schema(alerts_schema).csv("alerts_conditions.csv")

# Виконання cross join та фільтрації
alerts_conditions_df = windowed_avg_df.crossJoin(alerts_df) \
    .filter(
    ((col("humidity_min") == -999) | (col("avg_humidity") >= col("humidity_min"))) &
    ((col("humidity_max") == -999) | (col("avg_humidity") <= col("humidity_max"))) &
    ((col("temperature_min") == -999) | (col("avg_temp") >= col("temperature_min"))) &
    ((col("temperature_max") == -999) | (col("avg_temp") <= col("temperature_max")))
) \
    .select("window", "avg_temp", "avg_humidity", "code", "message")

# Виведення даних на екран
query = windowed_avg_df.writeStream \
    .trigger(processingTime='30 seconds') \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/checkpoints-6") \
    .start()

# Відправлення алертів у вихідний Kafka-топік
alerts_query = alerts_conditions_df \
    .selectExpr("CAST(window AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("topic", f"{my_name}_alerts") \
    .option("checkpointLocation", "/tmp/checkpoints-5") \
    .option("failOnDataLoss", "false") \
    .start() \
    .awaitTermination()

# # Виведення даних на екран
# query = windowed_avg_df.writeStream \
#     .trigger(processingTime='30 seconds') \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("checkpointLocation", "/tmp/checkpoints-4") \
#     .start() \
#     .awaitTermination()