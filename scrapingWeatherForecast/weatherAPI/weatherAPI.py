from flask import Flask
from flask_restful import Resource, Api, reqparse
import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

app = Flask(__name__)
api = Api(app)


class scrapeWeather(Resource):
    def __init__(self):
        self.cookies = {"CONSENT": "YES+cb.20210720-07-p0.en+FX+410"}

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('city', required=True)
        args = parser.parse_args()
        soup = BeautifulSoup(
            requests.get(f"https://www.google.com/search?q=weather+{args['city']}", cookies=self.cookies).content,
            "html.parser")
        time_sky_desc = soup.find('div', attrs={'class': 'BNeawe tAd8D AP7Wnd'}).text
        temp = soup.find('div', attrs={'class': 'BNeawe iBp4i AP7Wnd'}).text
        res_dict = {'temperature': temp, 'time_sky': time_sky_desc}
        return {'data': res_dict}, 200


api.add_resource(scrapeWeather, '/scrape_weather')

if __name__ == "__main__":
    app.run()
