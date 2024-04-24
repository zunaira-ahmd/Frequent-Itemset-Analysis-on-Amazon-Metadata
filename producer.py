from kafka import KafkaProducer
import json
import time

# Kafka broker address
bootstrap_servers = ['localhost:9092']

# Topic to publish data to
topic = 'preprocessed_data'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Function to read preprocessed data and publish it to Kafka
def publish_data():
    # Load preprocessed data from JSON file or database
    with open('Cleaned_Sampled_Amazon_Meta.json', 'r') as file:
        data = json.load(file)

    # Publish each record to Kafka topic
    for record in data:
        producer.send(topic, value=record)
        print("Published:", record)
        time.sleep(1)  # Simulate real-time streaming

    # Close the producer
    producer.close()

if __name__ == "__main__":
    publish_data()

