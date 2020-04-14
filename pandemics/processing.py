import pandas as pd
import numpy as np
from typing import *
import pandemics.utils
from datetime import datetime
from os.path import join
from geopy.geocoders import Nominatim

def jhu_world_normalize(df: pd.DataFrame) -> pd.DataFrame:
    # We are just gonna do per country data in this CSV file
    df = df.drop(columns=['Province/State'])
    # Change column names to the ones we use
    df = df.rename(columns={
        'Country/Region': 'country',
        'Lat': 'latitude',
        'Long': 'longitude'
    })

    # Remove duplicate countries
    dup_countries = {
        'Bahamas, The',
        'Congo (Brazzaville)'
    }

    df = df[~df.country.isin(dup_countries)]

    # Change Korea, South to South Koreaa
    df.country = df.country.replace({
        'Korea, South': 'South Korea',
        'US': 'United States',
        'The Bahamas': 'Bahamas',
        'Congo (Kinshasa)': 'Democratic Republic of the Congo',
        'Czechia': 'Czech Republic',
        'Taiwan*': 'Taiwan',
        'Cruise Ship': 'Diamond Princess',
        'Cote d\'Ivoire': 'Ivory Coast'
    })

    # JHU data has a PK of (region, country) so we need to sum up the rows that are dates for each one
    dup = df[df.duplicated(['country'])]
    dup = dup.groupby('country').sum()
    dup = dup.reset_index()
    dup.latitude = 0
    dup.longitude = 0

    # Unique countries
    df = df[~df.duplicated(['country'], keep=False)]
    df = df.append(dup)

    return df

def unh_world_normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.astype({
        'cases': 'Int64',
        'new_cases': 'Int64',
        'deaths': 'Int64',
        'serious_and_critical': 'Int64',
        'recovered': 'Int64'
    })
    df.country = df.country.replace({
        'Congo Republic': 'Republic of the Congo',
        'DR Congo': 'Democratic Republic of the Congo'
    })
    return df

def jhu_state_normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=['UID', 'iso2', 'iso3', 'code3', 'Country_Region', 'Combined_Key', 'Population'], errors='ignore')
    df = df.rename(columns={
        'Admin2': 'county',
        'Province_State': 'state',
        'Lat': 'latitude',
        'Long_': 'longitude',
        'FIPS': 'fips'
    })

    # Make state first column and county second column
    cols = df.columns.tolist()
    cols[0], cols[1] = cols[1], cols[0]
    df = df[cols]

    return df

def unh_state_normalize(df: pd.DataFrame) -> pd.DataFrame:

    df.state = df.state.str.strip('\'')

    drop_countries = {'American Samoa', 'Diamond Princess (repatriated)', 'Grand Princess',
                      'Guam', 'Northern Mariana Islands', 'U.S. Virgin Islands', 'Puerto Rico',
                      'Wuhan (repatriated)', }

    df = df[~df.state.isin(drop_countries)]
    
    return df

def nyt_county_normalize(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.rename(columns={
        'cases': 'confirmed'
    })

    # Change their dates to the date format we use
    # They use YYYY-MM-DD
    # We use M/D/YYYY
    def convert_nyt_date(date):
        old = datetime.strptime(date, '%Y-%m-%d')
        new = old.strftime('%-m/%-d/%y')
        return new

    df.date = df.date.map(convert_nyt_date)

    def split_nyt_data(df: pd.DataFrame) -> pd.DataFrame:
        deaths = df.loc[:, df.columns != 'cases']
        confirmed = df.loc[:, df.columns != 'deaths']
        return confirmed, deaths

    # Need to turn date column into a bunch of different columns for each state/county
    dates = list(set(df.date))
    dates.sort(key=lambda d: datetime.strptime(d, '%m/%d/%y'))

    # Split into two dataframes for cases and deaths
    confirmed, deaths = split_nyt_data(df)

    # Do the following for each of recovered and deaths
    def transpose_nyt_data(df: pd.DataFrame, expand: str) -> pd.DataFrame:
        join_on = ['county', 'state', 'fips']
        state_county = set(map(tuple, df[join_on].values))
        t = pd.DataFrame(state_county, columns=join_on)
        for date in dates:
            s = df[df.date == date][join_on + [expand]]
            s = s.rename(columns={expand: date})
            t = t.merge(s, on=join_on, how='left')
        return t

    confirmed = transpose_nyt_data(confirmed, expand='confirmed')
    deaths = transpose_nyt_data(deaths, expand='deaths')

     # make fips an str instead of a float
    confirmed = confirmed.astype({'fips': 'object'})
    deaths = deaths.astype({'fips': 'object'})

    confirmed.fips = confirmed.fips.astype('Int64').astype(str).str.zfill(5)
    deaths.fips = deaths.fips.astype('Int64').astype(str).str.zfill(5)

    table = pandemics.fetch.county_table()

    def geocode_nyt(df):
        df = pd.merge(df, table, how='left', on='fips')

        cols = df.columns.tolist()
        cols = cols[:3] + cols[-2:] + cols[3:-2]
        df = df[cols]

        date_cols = [col for col in df.columns if '/' in col]
        date_retype = {d: 'Int64' for d in date_cols}

        df = df.astype(date_retype)

        return df

    confirmed = geocode_nyt(confirmed)
    deaths = geocode_nyt(deaths)

    return confirmed, deaths

def split_jhu_state_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    
    state = df[df.duplicated(['state'])]
    state = state.groupby('state').sum()
    state = state.reset_index()
    state.latitude = 0.0
    state.longitude = 0.0

    geocoder = Nominatim(timeout=60)

    lat, lon = zip(*[pandemics.utils.geocode(geocoder, f'{state}, United States') for state in state.state])

    state.latitude = lat
    state.longitude = lon

    state = state.drop(columns=['fips'])

    return df, state


def split_unh_data(df: pd.DataFrame, pk: str = 'country') -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    date = pandemics.utils.timeseries_date()
    common_cols = [pk, 'latitude', 'longitude']
    recovered = df[common_cols + ['recovered']].rename(columns={'recovered': date})
    confirmed = df[common_cols + ['cases']].rename(columns={'cases': date})
    deaths = df[common_cols + ['deaths']].rename(columns={'deaths': date})
    return recovered, confirmed, deaths

def take_greatest(df: pd.DataFrame, pk: str = 'country') -> pd.DataFrame:
    to_drop = {col for col in df.columns if col.endswith('_jhu') or col.endswith('_unh')}
    dates = {col.split('_')[0] for col in df.columns if col != pk}

    for date in dates:
        df[date] = df.filter(like=date).max(axis=1)
    
    df = df.drop(columns=to_drop)
    # Sort the date columns by their datetime value
    
    date_cols = sorted(df.columns[3:], key=lambda d: datetime.strptime(d, '%m/%d/%y'))
    # Convert date columns to integer
    df = df.astype({d:'Int64' for d in date_cols})
    df = df[[pk, 'latitude', 'longitude'] + date_cols]

    # Make null lat/lons 0s
    df.latitude = df.latitude.fillna(0.0)
    df.longitude = df.longitude.fillna(0.0)

    # If the new number is lower than the old number, use the old number
    df.iloc[:, -1] = df.iloc[:, [-1, -2]].max(axis=1)
    df = df.astype({date_cols[-1]: 'Int64'})

    return df

def join_unh_jhu(df: pd.DataFrame, to_join: Union[pd.DataFrame, Iterable[pd.DataFrame]], pk: str = 'country', greatest: bool = True) -> pd.DataFrame:
    if isinstance(to_join, pd.DataFrame):
        to_join = [to_join]

    for j in to_join:
        df = df.merge(j, how='outer', on=pk, suffixes=['_jhu', '_unh'])
        df = df.drop(columns=['latitude_jhu', 'longitude_jhu'])
        df = df.rename(columns={
            'latitude_unh': 'latitude',
            'longitude_unh': 'longitude'
        })
    
    # Rearrange the data so latitude and longitude is at the beginning
    cols = list(df)
    cols.insert(1, cols.pop(cols.index('latitude')))
    cols.insert(2, cols.pop(cols.index('longitude')))
    df = df[cols]

    if greatest:
        df = take_greatest(df, pk=pk)

    return df

def get_jhu_world_data(path: str, normalize: bool = True) -> pd.DataFrame:
    df = pd.read_csv(path)
    if normalize:
        df = pandemics.processing.jhu_world_normalize(df)
    return df

def get_jhu_state_data(path: str, normalize: bool = True) -> pd.DataFrame:
    df = pd.read_csv(path)
    if normalize:
        df = pandemics.processing.jhu_state_normalize(df)
    return df

def get_world_update(jhu_timeseries_path: str, normalize: bool = True, greatest: bool = True) -> pd.DataFrame:

    print('getting world update')

    recovered_jhu = get_jhu_world_data(join(jhu_timeseries_path, 'time_series_covid19_recovered_global.csv'), normalize=normalize)
    confirmed_jhu = get_jhu_world_data(join(jhu_timeseries_path, 'time_series_covid19_confirmed_global.csv'), normalize=normalize)
    deaths_jhu = get_jhu_world_data(join(jhu_timeseries_path, 'time_series_covid19_deaths_global.csv'), normalize=normalize)

    print('got jhu world data, showing recovered')
    print(recovered_jhu.head())

    print('fetching unh world data')
    # Get the most recent world data (contains confirmed, deaths, and recovered all in one)
    unh_world = pandemics.fetch.world_data(normalize=normalize)

    print('got unh world data')
    print(unh_world.head())

    recovered_unh, confirmed_unh, deaths_unh = split_unh_data(unh_world)

    print('split unh data, showing recovered')
    print(recovered_unh.head())

    print('joining our data with jhu')
    recovered = join_unh_jhu(recovered_jhu, recovered_unh, greatest=greatest)
    confirmed = join_unh_jhu(confirmed_jhu, confirmed_unh, greatest=greatest)
    deaths = join_unh_jhu(deaths_jhu, deaths_unh, greatest=greatest)

    print('joined, showing combined recovered')
    print(recovered.head())

    return confirmed, recovered, deaths

def get_state_update(jhu_timeseries_path: str, normalize: bool = True, greatest: bool = True):

    print('getting state update')

    confirmed_jhu = get_jhu_state_data(join(jhu_timeseries_path, 'time_series_covid19_confirmed_US.csv'), normalize=normalize)
    deaths_jhu = get_jhu_state_data(join(jhu_timeseries_path, 'time_series_covid19_deaths_US.csv'), normalize=normalize)

    print('got jhu state data, showing confirmed')
    print(confirmed_jhu.head())

    confirmed_jhu_county, confirmed_jhu_state = split_jhu_state_data(confirmed_jhu)
    deaths_jhu_county, deaths_jhu_state = split_jhu_state_data(deaths_jhu)
    # JHU does not provide recovered US state data
    print('split jhu into county/state data, showing confirmed state')
    print(confirmed_jhu_state.head())

    print('fetching unh state data')
    unh_state = pandemics.fetch.state_data(normalize=normalize)

    print('got our state data')
    print(unh_state.head())

    recovered_unh, confirmed_unh, deaths_unh = split_unh_data(unh_state, pk='state')

    print('split our state data, showing confirmed')
    print(recovered_unh.head())

    confirmed_state = join_unh_jhu(confirmed_unh, confirmed_jhu_state, pk='state', greatest=greatest)
    deaths_state = join_unh_jhu(deaths_unh, deaths_jhu_state, pk='state', greatest=greatest)

    print('joined, showing combined confirmed')
    print(confirmed_state.head())

    return confirmed_state, deaths_state

def get_county_update(normalize: bool = True):
    confirmed, recovered = pandemics.fetch.county_data(normalize)
    return confirmed, recovered