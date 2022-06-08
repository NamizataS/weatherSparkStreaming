import requests
from bs4 import BeautifulSoup

if __name__ == "__main__":
    cookies = {"CONSENT": "YES+cb.20210720-07-p0.en+FX+410"}
    soup = BeautifulSoup(requests.get("https://www.google.com/search?q=weather+paris", cookies=cookies).content,
                         "html.parser")
    temp = soup.find('div', attrs={'class': 'BNeawe iBp4i AP7Wnd'}).text
    time_sky_desc = soup.find('div', attrs={'class': 'BNeawe tAd8D AP7Wnd'}).text
    print("{}\n{}".format(temp, time_sky_desc))
