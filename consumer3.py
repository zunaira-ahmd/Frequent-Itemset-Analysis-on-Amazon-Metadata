from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt

# Kafka broker address
bootstrap_servers = ['localhost:9092']

# Topic to consume data from
topic = 'preprocessed_data'

# Consumer group ID
consumer_group_id = 'consumer_group'

# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',  # Start reading at the earliest message
    group_id=consumer_group_id  
)

# Initialize lists to store data for visualization
x_values = []
y_values = []

# Read messages from Kafka and process data
for message in consumer:
    # Extract data from Kafka message
    data = message.value
    
    # Process data and extract relevant information
    x_value = data.get('x_value')
    y_value = data.get('y_value')
    
    # Append data to lists
    x_values.append(x_value)
    y_values.append(y_value)
    
    # Plot the data
    plt.plot(x_values, y_values)
    plt.xlabel('X-axis Label')
    plt.ylabel('Y-axis Label')
    plt.title('Real-time Data Visualization')
    plt.pause(0.1) 

    # Optionally save the plot to a file
    plt.savefig('real_time_plot.png')

    # Clear the plot for the next iteration
    plt.clf()
