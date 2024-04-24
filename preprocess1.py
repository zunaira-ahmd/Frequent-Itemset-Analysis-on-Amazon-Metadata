import json
import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

# Download stopwords if not already downloaded
nltk.download('stopwords')
nltk.download('punkt')

# Load stopwords
stop_words = set(stopwords.words('english'))

# Function to print column names
def print_column_names(input_file):
    with open(input_file, 'r', encoding='utf-8') as infile:
        first_record = json.loads(infile.readline())
        column_names = list(first_record.keys())
        print("Column Names:", column_names)

# Function to clean and format data
def clean_and_format_data(input_file, output_file, num_rows_to_output=5):
    cleaned_records = []  # List to store cleaned records
    with open(input_file, 'r', encoding='utf-8') as infile:
        for idx, line in enumerate(infile):
            record = json.loads(line)

            # Filter out only the desired columns
            filtered_record = {
                'title': record.get('title', None),
                'also_buy': record.get('also_buy', None),
                'also_view': record.get('also_view', None),
                'price': record.get('price', None),
                'asin': record.get('asin', None)
            }

            # Append cleaned record to the list
            cleaned_records.append(filtered_record)

            # Print a few rows of cleaned data
            if idx < num_rows_to_output:
                print("Cleaned Record #", idx + 1)
                print(filtered_record)
                print()

    # Write the cleaned and formatted records to the output file
    with open(output_file, 'w', encoding='utf-8') as outfile:
        json.dump(cleaned_records, outfile)

# Print column names
print_column_names('Sampled_Amazon_Meta.json')

# Clean and format the data
clean_and_format_data('Sampled_Amazon_Meta.json', 'Cleaned_Sampled_Amazon_Meta.json')

