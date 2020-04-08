import pandas as pd
import glob
import os
import csv
from functools import wraps
from datetime import datetime
from typing import *
from pathlib import Path
from geopy.geocoders.base import Geocoder
from geopy.exc import GeopyError
import shelve


def load_newest_csv(path: str) -> pd.DataFrame:
    csv_glob = os.path.join(path, '*.csv')
    csv_files = glob.glob(csv_glob)
    newest_csv = max(csv_files, key=os.path.getctime)
    return newest_csv, pd.read_csv(newest_csv)

def write_csv(path: str, rows: Iterable[Tuple], cols: Iterable[str]) -> None:
    build_path(path)
    path = os.path.join(path, time_fname())
    with open(path, 'w') as fp:
        writer = csv.writer(fp)
        writer.writerow(cols)
        for row in rows:
            writer.writerow(row)

def build_path(path: Union[str, Path]) -> None:
    if type(path) is str:
        path = Path(path)
    path.mkdir(exist_ok=True, parents=True)

def time_fname() -> str:
    return f"{datetime.now().strftime('%m-%d-%Y')}.csv"

def timeseries_date() -> str:
    return datetime.now().strftime('%-m/%-d/%y')

def try_int(s) -> Union[int, None]:
    try:
        s = s.replace(',', '')
        return int(s)
    except:
        return None

# Custom cache to disk based off the 2nd positional argument, used specifically for
# caching lats and lons below
def shelve_it(file_name):
    d = shelve.open(file_name)
    d['Bhutan'] = (27.5142, 90.4336) # NE
    d['Bosnia and Herzegovina'] = (43.9159, 17.6791) # NE
    d['Cabo Verde'] = (16.5388, -23.0418) #NW
    d['Central African Republic'] = (6.6111, 20.9394) # NE
    d['Chad'] = (15.4542, 18.7322) # NE
    d['Eswatini'] = (-26.5225, 31.4659) # SE
    d['Gambia'] = (13.4432, -15.3101) # NW
    d['Holy See'] = (41.9029, 12.4534) # NE
    d['Mauritania'] = (21.0079, -10.9408) # NW
    d['Nepal'] = (28.3949, 84.1240) # NE
    d['Nicaragua'] = (12.8654, -85.2072) # NW
    d['Papua New Guinea'] = (-6.3150, 143.9555) # SE
    d['Saint Vincent and the Grenadines'] = (12.9843, -61.2872) # NW
    d['Somalia'] = (5.1521, 46.1996) # NE
    d['Zimbabwe'] = (-19.0154, 29.1549) # SE
    d['Dominica'] = (15.4150, -61.3710) # NW
    d['Timor-Leste'] = (-8.8742, 125.7275) # SE
    d['Belize'] = (17.1899, -88.4976) # NW
    d['West Bank and Gaza'] = (31.9466, 35.3027) # NE
    d['Saint Kitts and Nevis'] = (17.3578, -62.7830) # NW
    d['Burma'] = (21.9162, 95.9560) # NE
    d['MS Zaandam'] = (26.0851, -80.1167) # NW
    d['Botswana'] = (-22.3285, 24.6849) # SE
    d['Burundi'] = (-3.3731, 29.9189) # SE
    d['Sierra Leone'] = (8.4606, -11.7799) # NW
    d['Malawi'] = (-13.2543, 34.3015) # SE
    d['South Sudan'] = (6.8770, 31.3070) # NE
    d['Western Sahara'] = (24.2155, -12.8858) # NW
    d['Georgia'] = (42.3154, 43.3569) # NE
    def decorator(func):
        def new_func(*args):
            loc = args[1]
            if loc not in d:
                d[loc] = func(*args)
            return d[loc]
        return new_func
    return decorator

@shelve_it('latlon.shelve')
def geocode(geocoder: Geocoder, location: str) -> Union[Tuple[float, float], Tuple[None, None]]:
    try:
        loc = geocoder.geocode(location)
    except GeopyError as e:
        return None, None
    else:
        lat = loc.latitude if loc else None
        lon = loc.longitude if loc else None
        return lat, lon