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

def cache(func):
    _cache = {}
    @wraps(func)
    def wrapper(*args, **kwargs):
        loc = args[1]
        data = _cache.get(loc)
        if data is None:
            data = func(*args, **kwargs)
            _cache[loc] = data
        return data
    return wrapper

# Custom cache to disk based off the 2nd positional argument, used specifically for
# caching lats and lons below
def shelve_it(file_name):
    d = shelve.open(file_name)
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