import pandas as pd
from elasticsearch import Elasticsearch
import os
import warnings 

warnings.filterwarnings("ignore")


# Correctly initializing the Elasticsearch client with HTTPS and authentication
client = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    http_auth=("elastic", "4AEh4SP8hKz4EVVI8x*p"),
    verify_certs=False  # Remember this should be True in production to avoid security risks
)

client.ping()
print('Connected to Elasticsearch')

def index_csv_file(file_path):
    # Load CSV file into DataFrame
    df = pd.read_csv(file_path)
    # Convert DataFrame to dictionary records
    records = df.to_dict(orient='records')
    
    # Index records into Elasticsearch
    for record in records:
        client.index(index=file_path.split('\\')[-1].lower(), document=record)

def index_text_file(file_path):
    # Read text file
    with open(file_path, 'r') as file:
        data = file.read()
    
    # Example of how you might want to structure the text data
    document = {
        "content": data
    }
    
    # Index the document into Elasticsearch
    client.index(index=file_path.split('\\')[-1].lower(), document=document)

def list_all_documents(index_name):
    # Define the search query to fetch all documents
    index_name = index_name.lower()
    query = {
        "query": {
            "match_all": {}  # This retrieves all documents
        }
    }
    # Execute the search query
    response = client.search(index=index_name, size=1000)  # Adjust size as needed
    documents = response['hits']['hits']  # Access the list of documents

    # Print document details
    for doc in documents:
        print(f"ID: {doc['_id']}, Content: {doc['_source']}")
        
def list_line_documents(index_name,field_name,field_value):
    # Define the search query to fetch all documents
    index_name = index_name.lower()
    query = {
        "query": {
            "match": {field_name: field_value}  # This retrieves all documents
        }
    }
    # Execute the search query
    response = client.search(index=index_name, body=query, size=1)
    documents = response['hits']['hits']  # Access the list of documents

    # Print document details
    for doc in documents:
        print(f"ID: {doc['_id']}, Content: {doc['_source']}")
        
def index_files_in_directory():
    # Directory containing your files
    directory = 'C:\\Users\\bdrkh\\Desktop20240508T223317BADER'

    # Loop through each file in the directory
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        
        # Check the file extension and call the appropriate function
        if filename.endswith('.csv'):
            index_csv_file(file_path)
        elif filename.endswith('.txt'):
            index_text_file(file_path)


#index_files_in_directory()
#list_all_documents("2024-05-08T22_33_17_6779741_ConsoleLog.txt")
#list_line_documents("2024-05-08T22_33_17_6779741_CopyLog.csv","SourceFile","C:\\Users\\Default\\NTUSER.DAT")