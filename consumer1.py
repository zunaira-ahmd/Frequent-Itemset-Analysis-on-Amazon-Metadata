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

min_confidence = 0.5

# Sliding window size
window_size = 1000
window = []

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

# Function to find frequent itemsets using Apriori algorithm
def apriori(data, min_support):
    L = []
    L1 = defaultdict(int)
    for transaction in data:
        for item in transaction:
            L1[item] += 1
    num_transactions = len(data)
    for item, support in L1.items():
        if support / num_transactions >= min_support:
            L.append([item])
    k = 2
    while L:
        Ck = generate_candidates(L)
        supports = calculate_support(data, Ck)
        L = [list(item) for item, support in supports.items() if support / num_transactions >= min_support]
        if L:
            yield L
        k += 1

# Function to discover associations between frequent itemsets
def discover_associations(frequent_itemsets, supports, num_transactions):
    associations = []
    for itemset in frequent_itemsets:
        if len(itemset) > 1:
            for i in range(1, len(itemset)):
                for subset in combinations(itemset, i):
                    antecedent = list(subset)
                    consequent = list(set(itemset) - set(antecedent))
                    support_A_B = supports[tuple(itemset)]
                    support_A = supports[tuple(antecedent)]
                    confidence = support_A_B / support_A
                    lift = confidence / (supports[tuple(consequent)] / num_transactions)
                    if confidence >= min_confidence:
                        associations.append((antecedent, consequent, support_A_B, confidence, lift))
    return associations

# Function to process sliding window and find associations
def process_sliding_window(window):
    frequent_itemsets = []
    for itemset in apriori(window, min_support):
        frequent_itemsets.extend(itemset)
        print("Frequent Itemsets:", itemset)
    
    # Discover associations
    supports = calculate_support(window, frequent_itemsets)
    num_transactions = len(window)
    associations = discover_associations(frequent_itemsets, supports, num_transactions)
    
    print("Associations:")
    for association in associations:
        antecedent, consequent, support, confidence, lift = association
        print(f"If {antecedent} then {consequent} (Support: {support}, Confidence: {confidence}, Lift: {lift})")

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
        process_sliding_window(window)
        # Shift the window
        window = window[1:]

# Close the consumer
consumer.close()
