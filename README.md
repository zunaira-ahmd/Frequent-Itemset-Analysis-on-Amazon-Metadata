# INTRODUCTION
Initially dealing with a dataset of 15GB, the data undergoes preprocessing and is subjected to algorithms like Apriori and PCY algorithms before being integrated into a non-relational database. The primary objective is to identify correlations and purchasing trends, followed by the storage of the large-scale data in databases.

## DEPENDENCIES
- kakfa
- apache spark
- mongodb
- jupyter notebook
- python



## Preprocessing the Data
Initially, the data is downloaded and subsequently preprocessed. The code is executed on a sample dataset of 15 gigabytes in size, which undergoes preprocessing. As a result, a new JSON file is generated to store the preprocessed data.


## Producers and Consumers
A producer script is crafted to stream the preprocessed data, and subsequently, three consumer scripts are created. The first consumer executes the Apriori algorithm, succeeded by the implementation of the PCY algorithm. A sliding window approach is adopted for these consumers. In contrast, the third consumer utilizes graphical visualization techniques to explore the data visually.

## Connection to the database
Each consumer script includes code that establishes a connection between the processed data and a non-relational database, specifically MongoDB in our case.
