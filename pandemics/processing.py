import pandas as pd
from typing import *
import pandemics.utils
from datetime import datetime
import os.path

_jhu_timeseries = 'csse_covid_19_data/csse_covid_19_time_series'

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

# Split our data into recovered, confirmed, and deaths
def split_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    date = pandemics.utils.timeseries_date()
    common_cols = ['country', 'latitude', 'longitude']
    recovered = df[common_cols + ['recovered']].rename(columns={'recovered': date})
    confirmed = df[common_cols + ['cases']].rename(columns={'cases': date})
    deaths = df[common_cols + ['deaths']].rename(columns={'deaths': date})
    return recovered, confirmed, deaths

def take_greatest(df: pd.DataFrame) -> pd.DataFrame:
    to_drop = {col for col in df.columns if col.endswith('_jhu') or col.endswith('_unh')}
    dates = {col.split('_')[0] for col in df.columns if col != 'country'}

    for date in dates:
        df[date] = df.filter(like=date).max(axis=1)
    
    df = df.drop(columns=to_drop)
    # Sort the date columns by their datetime value
    date_cols = sorted(df.columns[3:], key=lambda d: datetime.strptime(d, '%m/%d/%y'))
    # Convert date columns to integer
    df = df.astype({d:'Int64' for d in date_cols})
    df = df[['country', 'latitude', 'longitude'] + date_cols]

    # Make null lat/lons 0s
    df.latitude = df.latitude.fillna(0.0)
    df.longitude = df.longitude.fillna(0.0)

    return df

def join_unh_jhu(df: pd.DataFrame, to_join: Union[pd.DataFrame, Iterable[pd.DataFrame]], greatest=True) -> pd.DataFrame:
    if isinstance(to_join, pd.DataFrame):
        to_join = [to_join]
    for j in to_join:
        df = df.merge(j, how='outer', on='country', suffixes=['_jhu', '_unh'])
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
        df = take_greatest(df)

    return df

def get_jhu_world_data(path: str, normalize: bool = True) -> pd.DataFrame:
    df = pd.read_csv(path)
    if normalize:
        df = pandemics.processing.jhu_world_normalize(df)
    return df

def get_world_update(jhu_repo_path: str, normalize=True, greatest=True) -> pd.DataFrame:

    recovered_jhu = get_jhu_world_data(os.path.join(jhu_repo_path, f'{_jhu_timeseries}/time_series_covid19_recovered_global.csv'), normalize=normalize)
    confirmed_jhu = get_jhu_world_data(os.path.join(jhu_repo_path, f'{_jhu_timeseries}/time_series_covid19_confirmed_global.csv'), normalize=normalize)
    deaths_jhu = get_jhu_world_data(os.path.join(jhu_repo_path, f'{_jhu_timeseries}/time_series_covid19_deaths_global.csv'), normalize=normalize)

    # Get the most recent world data (contains confirmed, deaths, and recovered all in one)
    unh_world = pandemics.fetch.world_data(normalize=normalize)

    recovered_unh, confirmed_unh, deaths_unh = pandemics.processing.split_data(unh_world)

    recovered = pandemics.processing.join_unh_jhu(recovered_jhu, recovered_unh, greatest=greatest)
    confirmed = pandemics.processing.join_unh_jhu(confirmed_jhu, confirmed_unh, greatest=greatest)
    deaths = pandemics.processing.join_unh_jhu(deaths_jhu, deaths_unh, greatest=greatest)

    return confirmed, recovered, deaths