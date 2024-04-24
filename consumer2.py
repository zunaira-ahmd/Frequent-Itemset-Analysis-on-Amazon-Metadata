from kafka import KafkaConsumer
import json
from collections import defaultdict
from itertools import combinations

# Kafka broker address
bootstrap_servers = ['localhost:9092']

# Topic to consume data from
topic = 'preprocessed_data'

# Consumer group ID
group_id = 'consumer_group'


min_support = 0.2

# Sliding window size
window_size = 1000
window = []

# PCY parameters
hash_table_size = 1000
bucket_size = 10
hash_functions = [lambda x: hash(x) % hash_table_size, lambda x: (2 * hash(x) + 1) % hash_table_size]

# Function to generate candidate itemsets
def generate_candidates(Lk):
    candidates = []
    for i in range(len(Lk)):
        for j in range(i + 1, len(Lk)):
            # Joining step: combine two frequent itemsets if the first k-1 items are the same
            if Lk[i][:-1] == Lk[j][:-1]:
                candidates.append(list(set(Lk[i]) | set(Lk[j])))
    return candidates

# Function to calculate support for itemsets
def calculate_support(data, candidates):
    supports = defaultdict(int)
    for transaction in data:
        for candidate in candidates:
            if set(candidate).issubset(set(transaction)):
                supports[tuple(candidate)] += 1
    return supports

# Function to apply PCY algorithm
def PCY(data):
    hash_table = [0] * hash_table_size
    frequent_items = []
    
    # Count frequent items using the hash table
    for transaction in data:
        for item in transaction:
            for hash_func in hash_functions:
                hash_value = hash_func(item)
                hash_table[hash_value] += 1
                
    # Generate frequent items based on hash table counts
    for i, count in enumerate(hash_table):
        if count >= bucket_size:
            frequent_items.append(i)
    
    # Second Pass: Count frequent itemsets
    L1 = [item for item in frequent_items]
    L = [L1]
    k = 2
    while L:
        Ck = generate_candidates(L[-1])
        supports = calculate_support(data, Ck)
        L = [list(item) for item, support in supports.items() if support >= min_support * len(data)]
        if L:
            yield L
        k += 1

# Create Kafka consumer
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                         auto_offset_reset='earliest',
                         group_id=group_id)

# Consume messages and update sliding window
for message in consumer:
    data = message.value
    window.append(data)
    if len(window) >= window_size:
        for frequent_itemset in PCY(window):
            print("Frequent Itemsets:", frequent_itemset)
        # Shift the window
        window = window[1:]

# Close the consumer
consumer.close()
