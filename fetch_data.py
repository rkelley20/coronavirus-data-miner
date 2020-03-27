import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
from geopy.geocoders import Nominatim, DataBC
from geopy.exc import GeopyError
from pathlib import Path
import os
import inspect
import sys
import threading

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:10.0) Gecko/20100101 Firefox/10.0'
}

geocoder = Nominatim(user_agent=headers['User-Agent'], timeout=60)

def try_int(s):
    try:
        s = s.replace(',', '')
        return int(s)
    except:
        if s is not None:
            print(f'Failed to parse {s} as integer')
        return None

def try_geocode(location):
    try:
        loc = geocoder.geocode(location)
    except GeopyError as e:
        print(f'Failed to geocode {location}: {e}')
        return None, None
    else:
        lat = loc.latitude if loc else None
        lon = loc.longitude if loc else None
        return lat, lon

# Used to add the current date to the csvs being saved out
def fname():
    return f"{datetime.now().strftime('%m-%d-%Y')}.csv"

# Recursively create the children folders and return the full path
def build_path(path):
    print(f'Recursively building {path}')
    Path(path).mkdir(exist_ok=True, parents=True)
    return os.path.join(path, fname())

def write_csv(path, rows, cols):
    with open(build_path(path), 'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(cols)
        for row in rows:
            writer.writerow(row)

def us_state_data(path='data/us/state'):
    r = requests.get('https://docs.google.com/spreadsheets/u/0/d/e/2PACX-1vR30F8lYP3jG7YOq8es0PBpJIE5yvRVZffOyaqC0GgMBN6yt0Q-NI8pxS7hd1F9dYXnowSC6zpZmW9D/pubhtml/sheet?headers=false&gid=1902046093', headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')

    tbody = soup.find('tbody')
    raw_rows = tbody.find_all('tr')[5:64]
    rows = []

    for row in raw_rows:
        tds = row.find_all('td')
        state = tds[0].string
        cases = try_int(tds[1].string)
        deaths = try_int(tds[2].string)
        serious = try_int(tds[3].string)
        critical = try_int(tds[4].string)
        recovered = try_int(tds[5].string)

        # Do not want to falsley geocode cruise ship instances or cases where people were
        # repatriated back to their country
        if 'Princess' in state or 'repatriated' in state:
            lat, lon = None, None
        else:
            lat, lon = try_geocode(f'{state}, United States')
        
        rows.append((state, cases, deaths, serious, critical, recovered, lat, lat))
    
    rows.sort(key=lambda r: r[0])
    
    write_csv(path, rows, cols=('state', 'cases', 'deaths', 'serious', 'critical', 'recovered', 'latitude', 'longitude'))

def world_data(path='data/world'):
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

        lat, lon = try_geocode(country)

        rows.append((country, cases, new_cases, deaths, new_deaths,
                     percent_deaths, serious_and_critical, recovered,
                     lat, lon))
    
    rows.sort(key=lambda r: r[0])

    write_csv(path, rows, cols=('country', 'cases', 'new_cases', 'deaths', 'new_deaths', 
                                'percent_deaths', 'serious_and_critical', 'recovered',
                                'latitude', 'longitude'))

def canada_provinces_data(path='data/canada/province'):
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

        lat, lon = try_geocode(f'{province}, Canada')

        rows.append((province, cases, deaths, serious, critical, recovered, lat, lon))
    
    rows.sort(key=lambda r: r[0])
    
    write_csv(path, rows, cols=('province', 'cases', 'deaths', 'serious', 'critical', 'recovered', 'latitude', 'longitude'))

def _ny_conn_nj_parse(state, path):

    state_idx = {
        'new york': 0,
        'new jersey': 1,
        'connecticut': 2
    }

    r = requests.get('https://www.nbcnewyork.com/news/local/how-many-in-tri-state-have-tested-positive-for-coronavirus-here-are-latest-cases-by-the-numbers/2317721/', headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')

    table = soup.find_all('table')
    # Last 2 rows are totals, except for new jersey, which has an additional 'under investigation row'
    raw_rows = table[state_idx[state]].tbody.find_all('tr')[:-2 if state != 'new jersey' else -3]
    rows = []

    for row in raw_rows:
        tds = row.find_all('td')
        county = tds[0].string
        cases = try_int(tds[1].string)
        lat, lon = try_geocode(f'{county}, {state}')
        rows.append((county, cases, lat, lon))
    
    rows.sort(key=lambda r: r[0])
    
    write_csv(path, rows, cols=('county', 'cases', 'latitude', 'longitude'))

def new_york_data(path='data/us/new_york'):
    _ny_conn_nj_parse('new york', path)
    
def new_jersey_data(path='data/us/new_jersey'):
    _ny_conn_nj_parse('new jersey', path)

def connecticut_data(path='data/us/connecticut'):
    _ny_conn_nj_parse('connecticut', path)


data_downloaders = [obj for name,obj in inspect.getmembers(sys.modules[__name__]) 
                    if inspect.isfunction(obj) and name.endswith('_data')]

for downloader in data_downloaders:
    proc = threading.Thread(target=downloader)
    proc.start()




