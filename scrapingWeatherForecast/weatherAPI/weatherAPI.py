from flask import Flask
from flask_restful import Resource, Api, reqparse
import requests
from bs4 import BeautifulSoup


app = Flask(__name__)
api = Api(app)


class scrapeWeather(Resource):
    def __init__(self):
        self.cookies = {"CONSENT": "YES+cb.20210720-07-p0.en+FX+410"}
        self.headers = {'User-Agent': 'Mozilla/5.0'}

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('city', required=True)
        parser.add_argument('country', required=True)
        args = parser.parse_args()
        soup = BeautifulSoup(
            requests.get(f"https://www.google.com/search?q=weather+{args['city']}+{args['country']}&hl=en", cookies=self.cookies, headers=self.headers).content,
            "html.parser")
        time_sky_desc = soup.find('div', attrs={'class': 'BNeawe tAd8D AP7Wnd'}).text
        temp = soup.find('div', attrs={'class': 'BNeawe iBp4i AP7Wnd'}).text
        res_dict = {'city': args['city'], 'country': args['country'], 'temperature': temp, 'time_sky': time_sky_desc}
        return res_dict, 200


api.add_resource(scrapeWeather, '/scrape_weather')

if __name__ == "__main__":
    app.run()
