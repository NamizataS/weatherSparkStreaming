from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class ElasticSearchResources:
    def __init__(self):
        self.es_client = Elasticsearch(hosts="http://localhost:9200/")

    def generate_data(self, docs, index_name):
        for docu in docs:
            yield {
                "_index": index_name,
                "_source": {k: v if v else None for k, v in docu.items()},
            }

    def create_index(self, mapping, settings, index_name):
        self.es_client.indices.create(index=index_name, mappings=mapping, settings=settings)

    def load_data_in_index(self, documents, index_name):
        bulk(self.es_client, self.generate_data(documents, index_name))

    def delete_index(self, index_name):
        self.es_client.options(ignore_status=[400, 401, 404]).indices.delete(index=index_name)

    def query_index(self, query, type_query, index_name, size=0):
        match type_query:
            case "aggs":
                res = self.es_client.search(index=index_name, aggs=query, size=size)
                list_dict_res = res["aggregations"]
                return list_dict_res
