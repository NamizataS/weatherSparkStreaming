from KafkaResources import KafkaInteraction
from elasticsearch import Elasticsearch

if __name__ == "__main__":
    es_client = Elasticsearch(hosts="http://localhost:9200/")
    kafkaInteractions = KafkaInteraction()
    kafkaInteractions.get_message("clean_datas", es_client, "clean_datas")
