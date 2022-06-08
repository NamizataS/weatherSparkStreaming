from elasticsearch import Elasticsearch

if __name__ == "__main__":
    LOCAL = False
    es_client = Elasticsearch(hosts="http://localhost:9200/")
    print(es_client.ping())
