from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, window, avg, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def main():
    # Step 1: Initialize SparkSession
    spark = SparkSession.builder.appName("RealTimeWeatherProcessing").config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.3.7.jar").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # Step 2: Define the Schema
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("quantity_ordered", IntegerType(), True),
        StructField("price_each", DoubleType(), True),
        StructField("purchase_address", StringType(), True),
        StructField("order_date", TimestampType(), True)  # or TimestampType()
    ])

    # Step 3: Read from Kafka
    topic = "purchasedata"

    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", topic).option("startingOffsets", "latest").load()

    # Step 4: Deserialize JSON and Convert Timestamp
    weather_data = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", schema).alias("data")) \
        .select("data.*") \
        .withColumn("order_date", to_timestamp("order_date", "yyyy-MM-dd HH:mm:ss"))

    # Step 5: Apply Windowed Computations
    windowed_data = weather_data \
        .groupBy(
            window(col("order_date"), "10 seconds"),
            col("product")
        ) \
        .agg(avg(col("quantity_ordered")).alias("avg_ord"))

    # Step 6: Prepare Data for Writing
    output_data = windowed_data.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("product"),
        col("avg_ord")
    )

    # Step 7: Write to PostgreSQL
    jdbc_url = "jdbc:postgresql://postgres:5432/postgresdb"  # using service name as defined in docker-compose
    jdbc_properties = {
        "user": "postgresuser",
        "password": "postgrespass",
        "driver": "org.postgresql.Driver"
    }

    checkpoint_location = "/opt/bitnami/spark"

    '''query = output_data.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda batch_df, epoch_id: batch_df.write.jdbc(
            url=jdbc_url,
            table="purchase_aggregates",
            mode="append",
            properties=jdbc_properties
        )) \
        .option("checkpointLocation", checkpoint_location) \
        .format("console") \
        .save() \
        .start()

    query.awaitTermination()'''

            # Write DataFrame to PostgreSQL
        # Define the sink (replace 'jdbc' and 'url' with your actual sink configuration)
    query = (
        output_data.writeStream
        .outputMode("update")
        .foreachBatch(write_to_postgresql)  # Call a function to write to PostgreSQL
        .start()
    )

    # Start the streaming query
    query.awaitTermination()
def write_to_postgresql(batch_df, batch_id):
    # Define your PostgreSQL connection properties
    jdbc_url = "jdbc:postgresql://postgres:5432/postgresdb"
    jdbc_properties = {"user": "postgresuser", "password": "postgrespass", "driver": "org.postgresql.Driver"}

    # Write the batch DataFrame to PostgreSQL
    batch_df.write.jdbc(url=jdbc_url, table="purchase_aggregates", mode="append", properties=jdbc_properties)

if __name__ == "__main__":
    main()