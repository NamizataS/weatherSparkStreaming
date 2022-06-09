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
        "aggs": {
            "cities": {
                "terms": {
                    "field": "city",
                    "size": 10000
                }
            }
        },
        "size": 0
    }
    #res = es_client.search(index="cities_index", query=query)
    #print(res)
    #print(es_client.count(index="cities_index", query=query))
    print(es_client.search(index="cities_index", body=query))
