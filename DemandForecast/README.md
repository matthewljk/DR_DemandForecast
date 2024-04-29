# Demand Forecast

## Python ENV

Python environment is managed by `conda`. To create the environment, run:
```bash
conda env create -f environment.yml
```

To create python environment to run on CPU-only environment, run:
```bash
conda env create -f environment-cpu.yml
```

## ENV variables

To establish a connection with the database, create a `.env` file in the root directory with the following content:
```bash
host='<url to db>'
user='<username>'
pswd='<password>'
db='<database name>'
schema='<shcema name, usually public>'
```

## Data

- Data under `data/` is updated to 2024-03-25 Period 33. But should only use data before '2024-02-23'. Because since '2024-02-23', `Solar` is seperated from `Demand`.
- Data in `DB/RealTimeDPR` is from '2024-04-15', with `Solar`.

## Notebooks

1. `1_data_analysis.ipynb`: Do mathematical analysis to help determine param settings in models.
2. `2_*.ipynb`: Model training
    1. `2_1_lstm_simple.ipynb`: Simple LSTN, no fancy tricks.
    2. `2_2_lstm_bi.ipynb`: Bi-LSTM.
3. `3_*_retrain.ipynb`: Retrain the model with new data.
4. `4_*_forecast.ipynb`: Scripts to make prediction using selected trained model.
5. `5_forecast_performance.ipynb`: Test model forecast performance by comparing predicted values with real values.

## Output

Scalers and models are saved under `model/` directory, using datetime as subfolder name.

## Make Predictions (Crontab)

We use `Crontab` to automatically make predictions and upload to database. 