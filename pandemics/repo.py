import git
import shutil
from pathlib import Path
import pandas as pd
import pandemics.processing

def clone_jhu(force: bool = True) -> None:
    jhu_repo = Path('COVID-19/')
    if jhu_repo.exists():
        if not force:
            return
        shutil.rmtree(jhu_repo)
    git.Git('').clone('git@github.com:CSSEGISandData/COVID-19.git')

def jhu_recovered(normalize: bool = True) -> pd.DataFrame:
    df = pd.read_csv('COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv')
    if normalize:
        df = pandemics.processing.jhu_normalize(df)
    return df

def jhu_confirmed(normalize: bool = True) -> pd.DataFrame:
    df = pd.read_csv('COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')
    if normalize:
        df = pandemics.processing.jhu_normalize(df)
    return df

def jhu_deaths(normalize: bool = True) -> pd.DataFrame:
    df = pd.read_csv('COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
    if normalize:
        df = pandemics.processing.jhu_normalize(df)
    return df