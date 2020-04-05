import pandemics.repo
import pandemics.processing
import pandemics.utils
import pandemics.fetch
from datetime import datetime
import schedule
import shutil
import time
import git
from os.path import join

# Common paths to be used for the scheduled tasks
DATA_ROOT_DIR = '/srv/miner/'
UNH_REPO_PATH = join(DATA_ROOT_DIR, 'COVID19-DATA')
JHU_REPO_PATH = join(DATA_ROOT_DIR, 'COVID-19')
TIMESERIES_PATH = 'covid_19_timeseries_data/'

WORLD_RECOVERED_CSV = 'world_country_recovered.csv'
WORLD_CONFIRMED_CSV = 'world_country_confirmed.csv'
WORLD_DEATHS_CSV = 'world_country_deaths.csv'

STATE_RECOVERED_CSV = 'us_state_recovered.csv'
STATE_CONFIRMED_CSV = 'us_state_confirmed.csv'
STATE_DEATHS_CSV = 'us_state_deaths.csv'

WORLD_RECOVERED_PATH = join(TIMESERIES_PATH, WORLD_RECOVERED_CSV)
WORLD_CONFIRMED_PATH = join(TIMESERIES_PATH, WORLD_CONFIRMED_CSV)
WORLD_DEATHS_PATH = join(TIMESERIES_PATH, WORLD_DEATHS_CSV)

STATE_RECOVERED_PATH = join(TIMESERIES_PATH, STATE_RECOVERED_CSV)
STATE_CONFIRMED_PATH = join(TIMESERIES_PATH, STATE_CONFIRMED_CSV)
STATE_DEATHS_PATH = join(TIMESERIES_PATH, STATE_DEATHS_CSV)

REALTIME_FILES = [WORLD_RECOVERED_PATH, WORLD_CONFIRMED_PATH, WORLD_DEATHS_PATH,
                  STATE_RECOVERED_PATH, STATE_CONFIRMED_PATH, STATE_DEATHS_PATH]

def realtime_update():
   
    confirmed_global, recovered_global, deaths_global = pandemics.processing.get_world_update(JHU_REPO_PATH, normalize=True, greatest=True)
    confirmed_state, deaths_state = pandemics.processing.get_state_update(JHU_REPO_PATH, normalize=True, greatest=True)

    repo = pandemics.repo.clone_repo('git@github.com:unhcfreg/COVID19-DATA.git', UNH_REPO_PATH, force=False, use_ssh=True)

    # Save world and state data out to repo
    recovered_global.to_csv(WORLD_RECOVERED_PATH)
    confirmed_global.to_csv(WORLD_CONFIRMED_PATH)
    deaths_global.to_csv(WORLD_DEATHS_PATH)

    confirmed_state.to_csv(STATE_CONFIRMED_PATH)
    deaths_state.to_csv(STATE_DEATHS_PATH)

    # Push real time data
    update_time = datetime.now().strftime('%-m/%-d/%Y @ %H:%M')
    pandemics.repo.push_files(repo, files=REALTIME_FILES, msg=f'Automatic update {update_time}')
    

if __name__ == '__main__':
    
    pandemics.utils.build_path(DATA_ROOT_DIR)

    # Do both of our tasks when the service starts immediately
    pandemics.repo.clone_jhu(JHU_REPO_PATH, force=True)
    realtime_update()

    schedule.every(10).minutes.do(realtime_update)
    schedule.every(6).hours.do(pandemics.repo.clone_jhu, JHU_REPO_PATH, force=True)

    while True:
        schedule.run_pending()
        time.sleep(30)
    
    
    









