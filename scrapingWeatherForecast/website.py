import geopandas as geopd
import matplotlib.pyplot as plt
import pandas as pd
from elasticsearch import Elasticsearch


if __name__ == "__main__":
    query = {
        "match_all": {

        }
    }

    es_client = Elasticsearch(hosts="http://localhost:9200/")
    res = es_client.search(index="avg_weather", query=query)
    res = res['hits']['hits']
    df = pd.concat(map(pd.DataFrame.from_dict, res), axis=1)['_source'].T
    df["_id"] = [i["_id"] for i in res]
    print(df.columns)
    gdf = geopd.GeoDataFrame(df, geometry=geopd.points_from_xy(df.lng, df.lat))
    world = geopd.read_file(geopd.datasets.get_path('naturalearth_lowres'))
    ax = world.explore()
    gdf.explore('avg(temperatureFormatted)', cmap='Set2', legend=True)
    plt.show()


