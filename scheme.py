from elasticsearch import Elasticsearch

# Create an Elasticsearch client
es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'https'}],
    http_auth=("elastic", "4AEh4SP8hKz4EVVI8x*p"),
    verify_certs=False  # Remember this should be True in production to avoid security risks
)

es.ping()
print('Connected to Elasticsearch')

# Define the index name
index_name = "mft_kape"

# Define the mapping for the MFT KAPE files
mapping = {
    "mappings": {
        "properties": {
            "filename": {
                "type": "keyword"
            },
            "filesize": {
                "type": "long"
            },
            "created_date": {
                "type": "date"
            },
            "modified_date": {
                "type": "date"
            },
            "accessed_date": {
                "type": "date"
            },
            "attributes": {
                "type": "keyword"
            },
            "parent_directory": {
                "type": "keyword"
            },
            "extension": {
                "type": "keyword"
            },
            "full_path": {
                "type": "text"
            },
            "content": {
                "type": "text"
            }
        }
    }
}

# Create the index with the defined mapping
es.indices.create(index=index_name, body=mapping)