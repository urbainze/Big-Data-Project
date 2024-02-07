import csv
import json
import time
import logging
from kafka import KafkaProducer
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger('kafka-producer')

# Function to read data from a CSV file
def read_csv(file_path):
    with open(file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        data = [row for row in csv_reader]
    return data

# Function to generate purchase data 
def generate_purchase_data(row):
    order_date_str = row['Order Date']
    order_date = datetime.strptime(order_date_str, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')

    return {
        'city': row['city'],
        'product': row['Product'],  # Adjusted to 'product' for consistency
        'quantity_ordered': int(row['Quantity Ordered']),
        'price_each': float(row['Price Each']),
        'state': row['state'],
        'order_date': order_date
    }

# Callback for successful message send
def on_send_success(record_metadata):
    log.info(f"Message sent to {record_metadata.topic} on partition {record_metadata.partition} with offset {record_metadata.offset}")

# Callback for message send failure (e.g., KafkaError)
def on_send_error(excp):
    log.error('Error sending message', exc_info=excp)

# Serializer to convert to bytes for transmission
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Connect to the Kafka server within the Docker network
producer = KafkaProducer(bootstrap_servers=['localhost:29092'], value_serializer=json_serializer)

if __name__ == "__main__":
    csv_file_path = 'dataset.csv'  # Replace with the path to your CSV file

    # Read data from CSV file
    csv_data = read_csv(csv_file_path)

    while True:
        for row in csv_data:
            purchase_data = generate_purchase_data(row)
            # Asynchronously send the data and add callbacks
            future = producer.send('purchasedata', purchase_data)  # Adjusted topic to 'purchasedata'
            future.add_callback(on_send_success).add_errback(on_send_error)

        time.sleep(1)  # Creating a new data point every second
