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
DATA_ROOT_DIR = '/srv/miner/'
UNH_REPO_PATH = os.path.join(DATA_ROOT_DIR, 'COVID19-DATA')
JHU_REPO_PATH = os.path.join(DATA_ROOT_DIR, 'COVID-19')
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

    update_time = datetime.now().strftime('%-m/%-d/%Y @ %H:%M')
    pandemics.repo.push_files(repo, files=[RECOVERED_PATH, CONFIRMED_PATH, DEATHS_PATH], msg=f'Automatic update {update_time}')

def realtime_update():
   
    confirmed, recovered, deaths = pandemics.processing.get_world_update(JHU_REPO_PATH, normalize=True, greatest=True)

    repo = pandemics.repo.clone_repo('git@github.com:unhcfreg/COVID19-DATA.git', UNH_REPO_PATH, force=False, use_ssh=True)

    # Save the data out to the repo that will eventu
    recovered.to_csv(RECOVERED_CSV_PATH_PUBLIC)
    confirmed.to_csv(CONFIRMED_CSV_PATH_PUBLIC)
    deaths.to_csv(DEATHS_CSV_PATH_PUBLIC)

    # Push real time data
    update_time = datetime.now().strftime('%-m/%-d/%Y @ %H:%M')
    pandemics.repo.push_files(repo, files=[RECOVERED_CSV_PATH_PUBLIC, CONFIRMED_CSV_PATH_PUBLIC, DEATHS_CSV_PATH_PUBLIC], msg=f'Automatic update {update_time}')
    

if __name__ == '__main__':
    
    pandemics.utils.build_path(DATA_ROOT_DIR)

    # Do both of our tasks when the service starts immediately
    pandemics.repo.clone_jhu(JHU_REPO_PATH, force=True)
    realtime_update()

    #schedule.every().day.at('06:00').do(timeseries_update)
    schedule.every(10).minutes.do(realtime_update)
    schedule.every(6).hours.do(pandemics.repo.clone_jhu, JHU_REPO_PATH, force=True)

    while True:
        schedule.run_pending()
        time.sleep(30)
    
    
    









