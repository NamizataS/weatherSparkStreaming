from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd


def generate_data(docs):
    for docu in docs:
        yield {
            "_index": "cities_index",
            "_source": {k: v if v else None for k, v in docu.items()},
        }


if __name__ == "__main__":
    LOCAL = False
    es_client = Elasticsearch(hosts="http://localhost:9200/")
    print(es_client.ping())
    es_client.options(ignore_status=[400, 401, 404]).indices.delete(index="cities_index")
    mapping = {
        "properties": {
            "city": {
                "type": "text"
            },
            "location": {
                "type": "geo_point"
            },
            "country": {
                "type": "keyword"
            }
        }
    }
    es_client.indices.create(index="cities_index", mappings=mapping)
    df_cities = pd.read_csv("worldcities.csv")
    df_cities['location'] = df_cities.lat.astype(str).str.cat(df_cities.lng.astype(str), sep=',')
    df_cities_es = df_cities[['city', 'location', 'country']]
    documents = df_cities_es.to_dict(orient="records")
    bulk(es_client, generate_data(documents))
