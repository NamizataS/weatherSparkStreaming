from elasticsearchResources.ElasticSearchResources import ElasticSearchResources
import pandas as pd
from kafkaResources.KafkaResources import KafkaInteraction
import json
import requests


if __name__ == "__main__":
    es = ElasticSearchResources()
    '''
    es.delete_index("cities_index")
    mapping = {
        "properties": {
            "city": {
                "type": "keyword"
            },
            "location": {
                "type": "geo_point"
            },
            "country": {
                "type": "text"
            }
        }
    }
    settings = {"max_result_window": 1000000}
    es.create_index(mapping, settings, "cities_index")
    df_cities = pd.read_csv("worldcities.csv")
    df_cities['location'] = df_cities.lat.astype(str).str.cat(df_cities.lng.astype(str), sep=',')
    df_cities_es = df_cities[['city_ascii', 'location', 'country']]
    df_cities_es = df_cities_es.rename(columns={"city_ascii": "city"})
    documents = df_cities_es.to_dict(orient="records")
    es.load_data_in_index(documents, "cities_index")
    '''
    query = {
        "cities": {
            "terms": {
                "field": "city",
                "size": 46000
            }
        }
    }

    res = es.query_index(query, "aggs", "cities_index")["cities"]["buckets"]
    list_cities = [d['key'] for d in res if 'key' in d]
    kafkaInteractions = KafkaInteraction()
    #kafkaInteractions.create_topic("raw_datas")
    headers = {"Content-Type": "application/json"}
    for i in range(3, 6):
        city = json.dumps({"city": list_cities[i]})
        req = f"http://127.0.0.1:8081/scrape_weather"
        res_req = requests.post(req, data=city, headers=headers)
        message = json.dumps(res_req.json()).encode("utf-8")
        kafkaInteractions.send_message(message, "raw_datas")
    kafkaInteractions.close_producer_connection()

