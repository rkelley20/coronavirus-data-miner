# coronavirus-data-miner
Scrapes websites for the last coronavirus information across the world.

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