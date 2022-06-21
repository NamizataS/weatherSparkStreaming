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
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "location": {
                "type": "geo_point"
            },
            "country": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
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
        "cities_details": {
            "multi_terms": {
                "terms": [
                    {"field": "city.keyword"},
                    {"field": "country.keyword"}
                ], "size": 46000
            }
        }
    }

    res = es.query_index(query, "aggs", "cities_index")["cities_details"]["buckets"]
    list_cities = [d['key'] for d in res if 'key' in d]

    kafkaInteractions = KafkaInteraction()
    print("Topic created")
    #kafkaInteractions.create_topic("raw_datas")
    headers = {"Content-Type": "application/json"}
    res_tab = []
    for i in range(100):
        print(f"City is {list_cities[i][0]}")
        try:
            city = json.dumps({"city": list_cities[i][0], "country": list_cities[i][1]})
            req = f"http://127.0.0.1:8081/scrape_weather"
            res_req = requests.post(req, data=city, headers=headers)
            message = json.dumps(res_req.json()).encode("utf-8")
            res_tab.append(message)
        except:
            continue
    kafkaInteractions.send_message(res_tab, "raw_datas")
    kafkaInteractions.close_producer_connection()
