import pandemics.repo
import pandemics.processing
import pandemics.utils
import pandemics.fetch
from datetime import datetime
import schedule
import shutil
import time
import git
import os

# Common paths to be used for the scheduled tasks
UNH_REPO_PATH = '/srv/miner/COVID19-DATA/'
TIMESERIES_PATH = 'covid_19_timeseries_data/'
RECOVERED_CSV_NAME = 'world_country_recovered.csv'
CONFIRMED_CSV_NAME = 'world_country_confirmed.csv'
DEATHS_CSV_NAME = 'world_country_deaths.csv'
RECOVERED_PATH = os.path.join(TIMESERIES_PATH, RECOVERED_CSV_NAME)
CONFIRMED_PATH = os.path.join(TIMESERIES_PATH, CONFIRMED_CSV_NAME)
DEATHS_PATH = os.path.join(TIMESERIES_PATH, DEATHS_CSV_NAME)
RECOVERED_CSV_PATH_PUBLIC = os.path.join(UNH_REPO_PATH, RECOVERED_PATH)
CONFIRMED_CSV_PATH_PUBLIC = os.path.join(UNH_REPO_PATH, CONFIRMED_PATH)
DEATHS_CSV_PATH_PUBLIC = os.path.join(UNH_REPO_PATH, DEATHS_PATH)

def timeseries_update():
    """Should only run every 24 hours. This function assumes that the JHU repo exists. The JHU repo is re-cloned every
    6 hours by the repo cloner task. All data is saved to the COVID19-DATA directory.
    """
    pandemics.repo.clone_repo('git@github.com:unhcfreg/COVID19-DATA.git', path=UNH_REPO_PATH, force=False)
    repo = git.Repo(UNH_REPO_PATH)

    shutil.copyfile(CONFIRMED_CSV_PATH_PRIVATE, CONFIRMED_CSV_PATH_PUBLIC)
    shutil.copyfile(RECOVERED_CSV_PATH_PRIVATE, RECOVERED_CSV_PATH_PUBLIC)
    shutil.copyfile(DEATHS_CSV_PATH_PRIVATE, DEATHS_CSV_PATH_PUBLIC)

    pandemics.repo.push_files(repo, files=[RECOVERED_PATH, CONFIRMED_PATH, DEATHS_PATH], msg=f'Automatic update {datetime.now()}')

def realtime_update():
    recovered_jhu = pandemics.repo.jhu_recovered(normalize=True)
    confirmed_jhu = pandemics.repo.jhu_confirmed(normalize=True)
    deaths_jhu = pandemics.repo.jhu_deaths(normalize=True)

    # Get the most recent world data (contains confirmed, deaths, and recovered all in one)
    unh_world = pandemics.fetch.world_data(normalize=True)

    recovered_unh, confirmed_unh, deaths_unh = pandemics.processing.split_data(unh_world)

    recovered = pandemics.processing.join_unh_jhu(recovered_jhu, recovered_unh, greatest=True)
    confirmed = pandemics.processing.join_unh_jhu(confirmed_jhu, confirmed_unh, greatest=True)
    deaths = pandemics.processing.join_unh_jhu(deaths_jhu, deaths_unh, greatest=True)

    # Build the timeseries path if it doesn't already exist
    pandemics.utils.build_path(TIMESERIES_PATH)

    # Save the data out to the repo that will eventu
    recovered.to_csv(RECOVERED_PATH)
    confirmed.to_csv(CONFIRMED_PATH)
    deaths.to_csv(DEATHS_PATH)

    # Push real time data
    pandemics.repo.push_files(git.Repo(''), files=[RECOVERED_PATH, CONFIRMED_PATH, DEATHS_PATH], msg=f'Automatic update {datetime.now()}')
    

if __name__ == '__main__':

    # Make sure repo is cloned at the start
    pandemics.repo.clone_jhu(force=True)

    #schedule.every().day.at('06:00').do(timeseries_update)
    schedule.every(30).minutes.do(realtime_update)
    schedule.every(6).hours.do(pandemics.repo.clone_jhu, force=True)

    while True:
        schedule.run_pending()
        time.sleep(30)
    









