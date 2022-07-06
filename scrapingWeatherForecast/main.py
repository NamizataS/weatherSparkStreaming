import random
import time

from elasticsearchResources.ElasticSearchResources import ElasticSearchResources
from kafkaResources.KafkaResources import KafkaInteraction
import json
import requests


def main_run(kafkaInteractions):
    es = ElasticSearchResources()
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

    headers = {"Content-Type": "application/json"}
    res_tab = []
    random_cities = random.choices(list_cities, k=200)
    for i in range(200):
        print(f"City is {random_cities[i][0]}")
        try:
            city = json.dumps({"city": random_cities[i][0], "country": random_cities[i][1]})
            req = f"http://127.0.0.1:8081/scrape_weather"
            res_req = requests.post(req, data=city, headers=headers)
            query4 = {
                "bool": {
                    "must": [
                        {
                            "match": {"city": random_cities[i][0]}
                        },
                        {
                            "match": {"country": random_cities[i][1]}
                        }
                    ]
                }
            }
            res4 = es.query_index(query4, "query", "cities_index")[0]['_source']
            res_req = res_req.json()
            res_req['location'] = res4['location']
            message = json.dumps(res_req).encode("utf-8")
            res_tab.append(message)
        except:
            continue

    kafkaInteractions.send_message(res_tab, "raw_datas")
    kafkaInteractions.close_producer_connection()


if __name__ == "__main__":
    kafkaInteractions = KafkaInteraction()
    # kafkaInteractions.create_topic("raw_datas")
    # kafkaInteractions.create_topic("avg_weather")
    # kafkaInteractions.create_topic("clean_datas")
    print("Topic created")
    while True:
        main_run(kafkaInteractions)
        time.sleep(300)

