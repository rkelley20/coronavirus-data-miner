# coronavirus-data-miner
Coronavirus Data Miner for UNH's Pandemics project. This is a service that will continuously fetch Coronavirus data from multiple sources, normalize them, prep them, and distribute them to our public repositories.

## Installation

```zsh
pip install -r requirements.txt
```

## Running

Guaranteed compatible with Python 3.6.9+.

To start the service manually you can run the following command:

```zsh
python3 covid-data-service.py
```

To start it in the background you can use the following command:

```zsh
./run-miner.sh
```

## Notes

The default data directory is `/srv/miner/`, this directory will try to be created. In order for the program to create the directory you must invoke the service with `sudo`.
You do not need to invoke the service with `sudo` if the data directory is in an area where you have write/read access to.
