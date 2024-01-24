from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, window, avg, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def create_table_if_not_exists(jdbc_url, jdbc_properties):
    # Initialize SparkSession
    spark = SparkSession.builder.appName("TableCreation").getOrCreate()

    # Define the schema for the table
    schema = StructType([
        StructField("order_date", TimestampType(), True),
        StructField("product", StringType(), True),
        StructField("avg_ord", DoubleType(), True)
    ])

    # Create an empty DataFrame with the desired schema
    empty_df = spark.createDataFrame([], schema=schema)

    # Write the DataFrame to the target table, creating it if not exists
    empty_df.write.jdbc(url=jdbc_url,
                        table="purchase_aggregates",
                        mode="append",  # Change to "overwrite" if you want to recreate the table
                        properties=jdbc_properties)

    # Stop the SparkSession
    spark.stop()

def main():
    # Step 1: Initialize SparkSession
    spark = SparkSession.builder.appName("RealTimeWeatherProcessing").getOrCreate()

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
        .withColumn("order_date", to_timestamp("order_date", "yyyy-MM-dd HH:mm"))

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

    # Create the table if it doesn't exist
    create_table_if_not_exists(jdbc_url, jdbc_properties)

    # Start the streaming query
    checkpoint_location = "/opt/bitnami/spark"
    query = output_data.writeStream \
        .outputMode("update") \
        .format("console") \
        .foreachBatch(lambda batch_df, epoch_id: batch_df.write.jdbc(
            url=jdbc_url,
            table="purchase_aggregates",
            mode="append",
            properties=jdbc_properties
        )) \
        .option("checkpointLocation", checkpoint_location) \
        .format('console') \
        .start()
        
    query.awaitTermination()

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    main()