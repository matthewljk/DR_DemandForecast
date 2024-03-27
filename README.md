# DR_TelegramBot

## ML/DL forecast

Python environment is managed by `conda`. To create the environment, run:
```bash
conda env create -f environment.yml
```

To establish a connection with the database, create a `.env` file in the root directory with the following content:
```bash
host = '<url to db>'
user = '<username>'
pswd = '<password>'
db = '<database name>'
schema = '<shcema name, usually being 'public'>'
```

Data (DPR) updated to 2024-03-25 Period 33.

1. Data analysis: `data_analysis.ipynb`
2. Data processing demo: `net_demand.ipynb`
3. Model training: 
   - `lstm_simple.ipynb`
   - `lstm_bi.ipynb`
4. Forecasting: `forecast.ipynb` -> `src/forecast.py`
5. Compare predicted values with real values: `compare.ipynb`

Scalers and models are saved under `model/` directory, using datetime as subfolder name.

## Periodic prediction (Crontab)

We use `Crontab` to automatically make predictions and upload to database. `Crontab` configuration can be done by add whatever in `src/forecast.crontab` to `crontab -e`. It will automatically run `src/forecast.sh` to do the prediction, so do make sure `src/forecast.sh` is excutable. 