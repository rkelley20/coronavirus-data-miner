import git
import shutil
import os
from pathlib import Path
import pandas as pd
import pandemics.processing
from typing import *

JHU_REPO_PATH = '/home/urbana/repos/pandemics-data-miner/COVID-19'

def clone_repo(git_url: str, path: str, force: bool = True, use_ssh: bool = False) -> Union[git.Repo, None]:
    repo_path = Path(path)
    if repo_path.exists():
        if not force:
            return git.Repo(path)
        shutil.rmtree(repo_path)
    if use_ssh:
        key = os.path.expanduser('~/.ssh/id_rsa')
        repo = git.Repo.clone_from(git_url, path, env={'GIT_SSH_COMMAND': f'ssh -i {key}'})
    else:
        repo = git.Repo.clone_from(git_url, path)
    
    return repo

def clone_jhu(force: bool = True) -> None:
    return clone_repo('git@github.com:CSSEGISandData/COVID-19.git', JHU_REPO_PATH, force=force, use_ssh=False)

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