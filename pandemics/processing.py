import pandas as pd
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
    df = df.drop(columns=['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Country_Region', 'Combined_Key'])
    df = df.rename(columns={
        'Admin2': 'county',
        'Province_State': 'state',
        'Lat': 'latitude',
        'Long_': 'longitude'
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

    recovered_jhu = get_jhu_world_data(join(jhu_timeseries_path, 'time_series_covid19_recovered_global.csv'), normalize=normalize)
    confirmed_jhu = get_jhu_world_data(join(jhu_timeseries_path, 'time_series_covid19_confirmed_global.csv'), normalize=normalize)
    deaths_jhu = get_jhu_world_data(join(jhu_timeseries_path, 'time_series_covid19_deaths_global.csv'), normalize=normalize)

    # Get the most recent world data (contains confirmed, deaths, and recovered all in one)
    unh_world = pandemics.fetch.world_data(normalize=normalize)

    recovered_unh, confirmed_unh, deaths_unh = split_unh_data(unh_world)

    recovered = join_unh_jhu(recovered_jhu, recovered_unh, greatest=greatest)
    confirmed = join_unh_jhu(confirmed_jhu, confirmed_unh, greatest=greatest)
    deaths = join_unh_jhu(deaths_jhu, deaths_unh, greatest=greatest)

    return confirmed, recovered, deaths

def get_state_update(jhu_timeseries_path: str, normalize: bool = True, greatest: bool = True):

    confirmed_jhu = get_jhu_state_data(join(jhu_timeseries_path, 'time_series_covid19_confirmed_US.csv'), normalize=normalize)
    deaths_jhu = get_jhu_state_data(join(jhu_timeseries_path, 'time_series_covid19_deaths_US.csv'), normalize=normalize)

    confirmed_jhu_state, confirmed_jhu_county = split_jhu_state_data(confirmed_jhu)
    deaths_jhu_state, deaths_jhu_county = split_jhu_state_data(deaths_jhu)
    # JHU does not provide recovered US state data

    unh_state = pandemics.fetch.state_data(normalize=normalize)

    recovered_unh, confirmed_unh, deaths_unh = split_unh_date(unh_state)

    confirmed = join_unh_jhu(confirmed_unh, confirmed_jhu, pk='state', greatest=True)
    deaths = join_unh_jhu(deaths_unh, deaths_jhu, pk='state', greatest=True)

    return confirmed, deaths

    

