# coronavirus-data-miner
Coronavirus Data Miner for UNH's Pandemics project. This is a service that will continuously fetch Coronavirus data from multiple sources, normalize them, prep them, and distribute them to our public repositories.

## Installation

```zsh
pip install -r requirements.txt
```

Copy the covid-data.service file into your /etc/systemd/system/ folder

```zsh
cp covid-data.serivce /etc/systemd/system/
```



## Running

Guaranteed compatible with Python 3.6.9+.

To start the service manually you can run the following command:

```zsh
python3 covid-data-service.py
```

Otherwise, you can run the service (assuming you followed above steps) by doing the following

```zsh
sudo systemctl covid-data start
sudo systemctl covid-data enable
```

## Notes

The default data directory is `/srv/miner/`, this directory will try to be created. In order for the program to create the directory you must invoke the service with `sudo`.
You do not need to invoke the service with `sudo` if the data directory is in an area where you have write/read access to.
