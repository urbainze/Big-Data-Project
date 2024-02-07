from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, window, col,window,first
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def main():
    # Step 1: Initialize SparkSession
    spark = SparkSession.builder.appName("RealTimeWeatherProcessing").config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.3.7.jar").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # Step 2: Define the Schema
    schema = StructType([
        StructField("city", StringType(), True),
        StructField("product", StringType(), True),
        StructField("quantity_ordered", IntegerType(), True),
        StructField("price_each", DoubleType(), True),
        StructField("states", StringType(), True),
        StructField("order_date", TimestampType(), True)  # or TimestampType()
    ])

    # Step 3: Read from Kafka
    topic = "purchasedata"

    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", topic).option("startingOffsets", "latest").load()

    # Step 4: Deserialize JSON and Convert Timestamp
    weather_data = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", schema).alias("data")) \
        .select("data.*") \
        .withColumn("order_date", to_timestamp("order_date", "yyyy-MM-dd HH:mm:ss"))\
        .withColumn("money_spend",col('quantity_ordered')*col('price_each'))
    

   

            # Write DataFrame to PostgreSQL
        # Define the sink (replace 'jdbc' and 'url' with your actual sink configuration)
    query = (
        weather_data.writeStream
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