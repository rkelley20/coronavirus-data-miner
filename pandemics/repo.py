import git
import shutil
from pathlib import Path
import pandas as pd
import pandemics.processing
from typing import *

JHU_REPO_PATH = 'COVID-19'

def clone_repo(git_url: str, path: str, force: bool = True) -> None:
    repo = Path(path)
    if repo.exists():
        if not force:
            return
        shutil.rmtree(repo)
    git.Git('').clone(git_url)

def clone_jhu(force: bool = True) -> None:
    clone_repo('git@github.com:CSSEGISandData/COVID-19.git', JHU_REPO_PATH, force=force)

def push_files(repo: git.Repo, files: Iterable[str], msg: str = '') -> None:
    repo.git.add(files)
    repo.index.commit(msg)
    origin = repo.remote(name='origin')
    origin.push()

def jhu_recovered(normalize: bool = True) -> pd.DataFrame:
    df = pd.read_csv(f'{JHU_REPO_PATH}/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv')
    if normalize:
        df = pandemics.processing.jhu_normalize(df)
    return df

def jhu_confirmed(normalize: bool = True) -> pd.DataFrame:
    df = pd.read_csv(f'{JHU_REPO_PATH}/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')
    if normalize:
        df = pandemics.processing.jhu_normalize(df)
    return df

def jhu_deaths(normalize: bool = True) -> pd.DataFrame:
    df = pd.read_csv(f'{JHU_REPO_PATH}/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
    if normalize:
        df = pandemics.processing.jhu_normalize(df)
    return df