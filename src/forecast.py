# %% [markdown]
# # Forecast Net Demand

# %%
from keras.models import load_model
import tensorflow as tf
import glob
import joblib
from sklearn.preprocessing import StandardScaler
import numpy as np
import holidays
import datetime as dt
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv('.env')

# Get the values of host, user, pswd, db, and schema from the environment variables
host = os.getenv('host')
user = os.getenv('user')
pswd = os.getenv('pswd')
db = os.getenv('db')
schema = os.getenv('schema')


# Use the values as needed
engine = create_engine(
    f"postgresql://{user}:{pswd}@{host}/{db}?options=-csearch_path%3D{schema}", echo=False)
conn = engine.connect()

# %% [markdown]
# ## Import data from CSV to PostgreSQL
#
# This step is used for testing purposes.
#
# Set `IMPORT_DATA` to `False` to skip this step.

# %%
IMPORT_DATA = False

# %%

if IMPORT_DATA:

    # Load and filer data from csv file

    rt_dpr = pd.read_csv('./data/RT_DPR.csv')
    rt_dpr = rt_dpr[['Date', 'Period', 'Demand', 'TCL', 'Transmission_Loss']]
    rt_dpr['Transmission_Loss'] = rt_dpr['Transmission_Loss'].fillna(0)
    rt_dpr = rt_dpr[rt_dpr['Date'] > '2023-06-30']
    rt_dpr = rt_dpr.sort_values(by=['Date', 'Period'])
    rt_dpr.reset_index(drop=True, inplace=True)

    vc_per = pd.read_csv('./data/VCData_Period.csv')

    # !!! The Real_Time_DPR table here is different from the one Matthew uses. Don't replace.
    # rt_dpr.to_sql('Real_Time_DPR', conn, if_exists='replace', index=False)
    vc_per.to_sql('VCData_Period', conn, if_exists='replace', index=False)

# %% [markdown]
# ## Data from DB

# %%

now = dt.datetime.now()
date = now.strftime("%Y-%m-%d")
time = now.strftime("%H:%M")

period = int(now.strftime("%H")) * 2 + int(now.strftime("%M")) // 30 + 1


if period + 1 > 48:
    next_period = 1
    next_date = now + dt.timedelta(days=1)
    next_date = next_date.strftime("%Y-%m-%d")
else:
    next_period = period + 1
    next_date = date

# next_date = '2024-03-25' # A hard-coded value for testing
# next_period = 33 # A hard-coded value for testing
print(f"Now is {date} {time} Period {period}")
print(f"To predict: {next_date} Period {next_period}")

# %%
rt_dpr = pd.read_sql(f"""
                     SELECT "Date", "Period", "Demand", "TCL", "Transmission_Loss"
	                 FROM public."Real_Time_DPR"
                     WHERE ("Date" < '{date}' OR ("Date" = '{date}' AND "Period" < {next_period}))
                     ORDER BY "Date" DESC, "Period" DESC  
                     LIMIT 336
                     """, conn)
rt_dpr.sort_values(by=['Date', 'Period'], inplace=True)
rt_dpr.reset_index(drop=True, inplace=True)
rt_dpr.head(2)

# %%
rt_dpr.tail(2)

# %%
vc_per = pd.read_sql('SELECT * FROM public."VCData_Period"', conn)
vc_per

# %%
vc_per[(vc_per['Year'] == 2024) & (
    vc_per['Quarter'] == 1)]['TCQ_Weekday'].values

# %%

# Calculate required data fields

sg_holidays = holidays.country_holidays('SG')

rt_dpr['Total Demand'] = rt_dpr['Demand'] + \
    rt_dpr['TCL'] + rt_dpr['Transmission_Loss']
view = rt_dpr[['Date', 'Period', 'Total Demand']].copy()


def find_tcq(row):
    # print(row)
    date_obj = dt.datetime.strptime(str(row['Date']), '%Y-%m-%d')
    year = date_obj.year
    quarter = (date_obj.month - 1) // 3 + 1

    period = row['Period']

    isWeekend = 1 if date_obj.isoweekday() > 5 else 0
    isPublicHoliday = date_obj in sg_holidays

    if isWeekend or isPublicHoliday:
        # print(f"Date: {date_obj} isWeekend: {isWeekend} isPublicHoliday: {isPublicHoliday}")
        tcq = vc_per[(vc_per['Year'] == year) & (vc_per['Quarter'] == quarter) & (
            vc_per['Period'] == period)]['TCQ_Weekday'].values[0] / 1000
    else:
        tcq = vc_per[(vc_per['Year'] == year) & (vc_per['Quarter'] == quarter) & (
            vc_per['Period'] == period)]['TCQ_Weekend_PH'].values[0] / 1000

    # print(f"Date: {date_obj} TCQ: {tcq}")
    return tcq


view['TCQ'] = view.apply(lambda row: find_tcq(row), axis=1)
view['Net Demand'] = view['Total Demand'] - view['TCQ']
view.reset_index(drop=True, inplace=True)
# view.head(2)

# %%

# Load the most recent scaler file
resDir = './model'
newestDir = max(glob.glob(os.path.join(resDir, '*/')), key=os.path.getmtime)
newestDir

# %%
scaler_files = glob.glob(os.path.join(newestDir, "*.pkl"))
print("Scaler files:", scaler_files)
scaler = joblib.load(scaler_files[0])
print("Loaded scaler:", scaler_files[0])

# Perform data preprocessing as before
data = view.copy()
data['Target'] = data['Net Demand']
data['Target'] = scaler.fit_transform(data['Target'].values.reshape(-1, 1))

# Create dataset for prediction


def create_dataset(dataset):
    return np.array([dataset])


predict_X = create_dataset(data['Target'].values)

# Reshape input to be [samples, time steps, features]
predict_X = np.reshape(predict_X, (predict_X.shape[0], predict_X.shape[1], 1))
print(f"Predict_X shape: {predict_X.shape}")

# %% [markdown]
# ## Predict using trained model

# %%

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
tf.keras.utils.disable_interactive_logging()

print("Num GPUs Available: ", len(
    tf.config.experimental.list_physical_devices('GPU')))

# %%

# Get a list of all model files in the directory
model_files = glob.glob(os.path.join(newestDir, "*.keras"))

# Sort the list of model files by modification time (most recent first)
model_files.sort(key=os.path.getmtime, reverse=True)

# Select the most recent model file
most_recent_model_file = model_files[0]

# Load the selected model
model = load_model(most_recent_model_file)

# Print the path of the loaded model for verification
print("Loaded model:", most_recent_model_file)


# Make predictions
predict_result = model.predict(predict_X)

# Invert predictions to original scale
inverted_predictions = scaler.inverse_transform(predict_result)


# %%
# Print or use the predictions as needed
print(f"Predictions: {inverted_predictions[0][0]}")

# %%
