import json
import sys

import requests
from bs4 import BeautifulSoup
import pandas as pd
from elasticsearch import Elasticsearch

if __name__ == "__main__":
    '''
    cookies = {"CONSENT": "YES+cb.20210720-07-p0.en+FX+410"}
    soup = BeautifulSoup(requests.get("https://www.google.com/search?q=weather+paris", cookies=cookies).content,
                         "html.parser")
    temp = soup.find('div', attrs={'class': 'BNeawe iBp4i AP7Wnd'}).text
    time_sky_desc = soup.find('div', attrs={'class': 'BNeawe tAd8D AP7Wnd'}).text
    print("{}\n{}".format(temp, time_sky_desc))
    
    df = pd.read_csv("worldcities.csv")
    df['location'] = df.lat.astype(str).str.cat(df.lng.astype(str), sep=',')
    documents = df.to_dict(orient="records")
    print(documents[0:2])
    new_df = df[['city', 'location', 'country']]
    print(new_df.head())
    '''
    es_client = Elasticsearch(hosts="http://localhost:9200/")
    print(es_client.ping())
    print(es_client.indices.get(index="cities_index"))
    query = {
        "cities": {
            "terms": {
                "field": "city.keyword",
                "size": 46000
            }
        }
    }

    query2 = {
        "cities_details": {
            "multi_terms": {
                "terms": [
                    {"field": "city.keyword"},
                    {"field": "country.keyword"}
                ], "size": 46000
            }
        }
    }
    # res = es_client.search(index="cities_index", query=query)
    # print(res)
    # print(es_client.count(index="cities_index", query=query))

    # print(res_req.json())
    # print(json.dumps(res_req.json()).encode("utf-8"))
    # print(sys.getsizeof(json.dumps(res_req.json()).encode("utf-8")))
    res2 = es_client.search(index="cities_index", aggs=query2)
    list_dict_res2 = res2["aggregations"]["cities_details"]["buckets"]
    list_cities_with_details = [d['key'] for d in list_dict_res2 if 'key' in d]
    print(list_cities_with_details[0])
    headers = {"Content-Type": "application/json"}
    city = json.dumps({"city": list_cities_with_details[0][0], "country": list_cities_with_details[0][1]})
    req = f"http://127.0.0.1:8081/scrape_weather"
    res_req = requests.post(req, data=city, headers=headers)
    print(res_req.json())

    soup = BeautifulSoup(
        requests.get(f"https://www.google.com/search?q=weather+Marion+united states&hl=en",
                     cookies={"CONSENT": "YES+cb.20210720-07-p0.en+FX+410"}).content,
        "html.parser")
    time_sky_desc = soup.find('div', attrs={'class': 'BNeawe tAd8D AP7Wnd'}).text
    # temp = soup.find('div', attrs={'class': 'vk_bk TylWce SGNhVe'}).text
    # print(temp)
    query3 = {
        "cities_details": {
            "multi_terms": {
                "terms": [
                    {"field": "city.keyword"},
                    {"field": "country.keyword"},
                ], "size": 46000
            }
        }
    }
    res3 = es_client.search(index="cities_index", aggs=query3)
    list_dict_res3 = res3["aggregations"]["cities_details"]["buckets"]
    list_cities_with_details = [d['key'] for d in list_dict_res3 if 'key' in d]
    print(list_cities_with_details[0])

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

