import requests
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.geocoders.base import Geocoder
import pandas as pd
from pandemics.utils import geocode, try_int, write_csv
import pandemics.processing

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:10.0) Gecko/20100101 Firefox/10.0'
}

geocoder = Nominatim(user_agent=headers['User-Agent'], timeout=60)

def set_geocoder(new_geocoder: Geocoder) -> None:
    """Sets the geocoder to use for geolocation.

    Args:
        new_geocoder (Geocoder): The geocoder to use. Must be a valid geopy geocoder.
    """
    global geocoder
    geocoder = new_geocoder

def state_data(normalize: bool = True) -> pd.DataFrame:
    r = requests.get('https://docs.google.com/spreadsheets/u/0/d/e/2PACX-1vR30F8lYP3jG7YOq8es0PBpJIE5yvRVZffOyaqC0GgMBN6yt0Q-NI8pxS7hd1F9dYXnowSC6zpZmW9D/pubhtml/sheet?headers=false&gid=1902046093', headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')

    tbody = soup.find('tbody')
    raw_rows = tbody.find_all('tr')[5:64]
    rows = []

    for row in raw_rows:
        tds = row.find_all('td')
        state = tds[0].string
        cases = try_int(tds[1].string)
        deaths = try_int(tds[3].string)
        recovered = try_int(tds[7].string)

        lat, lon = geocode(geocoder, f'{state}, United States')
        
        rows.append((state, cases, deaths, recovered, lat, lon))
    
    rows.sort(key=lambda r: r[0])
    
    df = pd.DataFrame(rows, columns=('state', 'cases', 'deaths', 'recovered', 'latitude', 'longitude'))
    if normalize:
        df = pandemics.processing.unh_state_normalize(df)
    return df

def world_data(normalize: bool = True) -> pd.DataFrame:
    r = requests.get('https://docs.google.com/spreadsheets/u/0/d/e/2PACX-1vR30F8lYP3jG7YOq8es0PBpJIE5yvRVZffOyaqC0GgMBN6yt0Q-NI8pxS7hd1F9dYXnowSC6zpZmW9D/pubhtml/sheet?headers=false&gid=0&range=A1:I183', headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')

    tbody = soup.find('tbody')
    raw_rows = tbody.find_all('tr')[7:-3]
    rows = []

    for row in raw_rows:
        tds = row.find_all('td')
        country = tds[0].string
        cases = try_int(tds[1].string)
        new_cases = try_int(tds[2].string)
        deaths = try_int(tds[3].string)
        new_deaths = try_int(tds[4].string)
        percent_deaths = float(tds[5].string[:-1]) * .01
        percent_deaths = round(percent_deaths, 4)
        serious_and_critical = try_int(tds[6].string)
        recovered = try_int(tds[7].string)

        lat, lon = geocode(geocoder, country)
        rows.append((country, cases, new_cases, deaths, new_deaths,
                     percent_deaths, serious_and_critical, recovered,
                     lat, lon))
    
    rows.sort(key=lambda r: r[0])

    df = pd.DataFrame(rows, columns=('country', 'cases', 'new_cases', 'deaths', 'new_deaths', 
                                'percent_deaths', 'serious_and_critical', 'recovered',
                                'latitude', 'longitude'))
    if normalize:
        df = pandemics.processing.unh_world_normalize(df)
    return df

def canada_province_data() -> pd.DataFrame:
    r = requests.get('https://docs.google.com/spreadsheets/u/0/d/e/2PACX-1vR30F8lYP3jG7YOq8es0PBpJIE5yvRVZffOyaqC0GgMBN6yt0Q-NI8pxS7hd1F9dYXnowSC6zpZmW9D/pubhtml/sheet?headers=false&gid=338130207', headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')

    tbody = soup.find('tbody')
    raw_rows = tbody.find_all('tr')[5:-1]
    rows = []

    for row in raw_rows:
        tds = row.find_all('td')

        province = tds[0].string
        cases = try_int(tds[1].string)
        deaths = try_int(tds[2].string)
        serious = try_int(tds[3].string)
        critical = try_int(tds[4].string)
        recovered = try_int(tds[5].string)

        lat, lon = geocode(geocoder, f'{province}, Canada')

        rows.append((province, cases, deaths, serious, critical, recovered, lat, lon))
    
    rows.sort(key=lambda r: r[0])
    
    return pd.DataFrame(rows, columns=('province', 'cases', 'deaths', 'serious', 'critical', 'recovered', 'latitude', 'longitude'))
