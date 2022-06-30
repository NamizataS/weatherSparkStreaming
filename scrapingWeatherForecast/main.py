import time

from elasticsearchResources.ElasticSearchResources import ElasticSearchResources
import pandas as pd
from kafkaResources.KafkaResources import KafkaInteraction
import json
import requests


def main_run():
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

    kafkaInteractions = KafkaInteraction()
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


if __name__ == "__main__":
    kafkaInteractions = KafkaInteraction()
    kafkaInteractions.create_topic("raw_datas")
    kafkaInteractions.create_topic("avg_weather")
    print("Topic created")
    while True:
        main_run()
        time.sleep(300)

