# coronavirus-data-miner
Scrapes websites for the last coronavirus information across the world.

## Installation

```zsh
pip install -r requirements.txt
```

## Running

Guaranteed compatible with Python 3.6.9+.

Retrieve the most update to date data and save it out as CSV files.
```zsh
python fetch_data.py
```

Merge the data together with JHU data and save it out as CSVS.
```zsh
python merge_world_data.py
```

The above command will create a `data` directory in the current working directory with all available CSV files.