from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pymongo import MongoClient
from pyspark import SparkContext
import json

spark = SparkSession.builder.appName("MongoDB-Spark Connector").getOrCreate()

mongo_uri = "mongodb://localhost:27017"
client = MongoClient(mongo_uri)

# Select/Create database
db = client['metadata']

# create table
collection = db['amazon_metadata']

# store data in table
with open('cleaned_sampled_amazon_meta.json', 'r') as file:
    # Load the JSON data from the file
    data = json.load(file)

# Insert multiple documents
collection.insert_many(data)

spark.stop()

# Initialize SparkContext
sc = SparkContext("local", "MongoDB RDD")

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['ipldata']
collection = db['players']
