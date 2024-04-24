# %% [markdown]
# # Forecast Net Demand

# %%
# Set to true when running by CronTab
crontab = True

# %%
ROOT = '/home/sdc/DR_DemandForecast/DemandForecast'
if not crontab: ROOT = '.'

# %%
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load the environment variables from the .env file
env_file = f'{ROOT}/src/.env' if crontab else '.env'
load_dotenv(env_file, override=True)

# Get the values of host, user, pswd, db, and schema from the environment variables
host = os.getenv('host')
user = os.getenv('user')
pswd = os.getenv('pswd')
db = os.getenv('db')
schema = 'public'


#  Use the values as needed
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
IMPORT_DATA = True

# %%
import pandas as pd
import datetime as dt

if IMPORT_DATA:
    
    # Load and filer data from csv file
    
    rt_dpr = pd.read_csv(f'{ROOT}/data/RT_DPR.csv')
    rt_dpr = rt_dpr[['Date', 'Period', 'Demand', 'TCL', 'Transmission_Loss']]
    rt_dpr['Transmission_Loss'] = rt_dpr['Transmission_Loss'].fillna(0)
    rt_dpr = rt_dpr[rt_dpr['Date'] > '2023-06-30']
    rt_dpr = rt_dpr.sort_values(by=['Date', 'Period'])
    rt_dpr.reset_index(drop=True, inplace=True)
    
    vc_per = pd.read_csv(f'{ROOT}/data/VCData_Period.csv')
    
    # !!! The Real_Time_DPR table here is different from the one Matthew uses. Don't replace.
    # rt_dpr.to_sql('Real_Time_DPR', conn, if_exists='replace', index=False)
    vc_per.to_sql('VCData_Period', conn, if_exists='replace', index=False)

# %% [markdown]
# ## Data from DB

# %%
import datetime as dt
import pytz

now = dt.datetime.now(pytz.timezone('Asia/Singapore'))
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

# next_date = '2024-03-27' # A hard-coded value for testing
# next_period = 22 # A hard-coded value for testing
print(f"# @ {date} {time} Period {period} -> Predict: {next_date} Period {next_period}")

# %%
rt_dpr = pd.read_sql(f"""
SELECT "Date", "Period", "Demand", "TCL", "TransmissionLoss", "Solar"
FROM public."RealTimeDPR"
WHERE ("Date" < '{date}' OR ("Date" = '{next_date}' AND "Period" < {next_period}))
ORDER BY "Date" DESC, "Period" DESC  
LIMIT 336
""", conn)
rt_dpr.sort_values(by=['Date', 'Period'], inplace=True)
rt_dpr.reset_index(drop=True, inplace=True)
rt_dpr.fillna(0, inplace=True)

rt_dpr.iloc[[0, -1]]

# %%
vc_per = pd.read_sql('SELECT * FROM public."VCDataPeriod"', conn)
vc_per.iloc[[0, -1]]

# %% [markdown]
# ## Construct input data

# %%
import holidays

# Calculate required data fields

sg_holidays = holidays.country_holidays('SG')

rt_dpr['Total Demand'] = rt_dpr['Demand'] + rt_dpr['TCL'] + rt_dpr['TransmissionLoss'] + rt_dpr['TransmissionLoss']+ rt_dpr['Solar']
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
        tcq = vc_per[(vc_per['Year'] == year) & (vc_per['Quarter'] == quarter) & (vc_per['Period'] == period)]['TCQ_Weekday'].values[0] / 1000
    else:
        tcq = vc_per[(vc_per['Year'] == year) & (vc_per['Quarter'] == quarter) & (vc_per['Period'] == period)]['TCQ_Weekend_PH'].values[0] / 1000

    # print(f"Date: {date_obj} TCQ: {tcq}")
    return tcq

view['TCQ'] = view.apply(lambda row: find_tcq(row), axis=1)
view['Net Demand'] = view['Total Demand'] - view['TCQ']
view.reset_index(drop=True, inplace=True)


# %%
view

# %% [markdown]
# ### Debug: Copying data into shape of 336

# %%
# import numpy as np
# repeat_count = 336 // 268
# remainder = 336 % 268
# repeating_index = np.concatenate([np.repeat(np.arange(268), repeat_count), np.arange(remainder)])
# repeating_index
# view = view.iloc[repeating_index].reset_index(drop=True)
# view


# %% [markdown]
# ## Load scaler

# %%
import numpy as np
from sklearn.preprocessing import StandardScaler
import joblib
import os
import glob

# Load the most recent scaler file
resDir = f'{ROOT}/model'
newestDir = max(glob.glob(os.path.join(resDir, '*/')), key=os.path.getmtime)
# newestDir = './model/20240325_1527/'
if not crontab: print(newestDir)

# %%
scaler_files = glob.glob(os.path.join(newestDir, "*.pkl"))
if not crontab: print("Scaler files:", scaler_files)
scaler = joblib.load(scaler_files[0])
if not crontab: print("Loaded scaler:", scaler_files[0])

# Transform data using the loaded scaler
data = view.copy()
data['Target'] = data['Net Demand']
data['Target'] = scaler.fit_transform(data['Target'].values.reshape(-1,1))

def create_dataset(dataset):
    return np.array([dataset])

predict_X = create_dataset(data['Target'].values)

# Reshape input to be [samples, time steps, features]
predict_X = np.reshape(predict_X, (predict_X.shape[0], predict_X.shape[1], 1))

if not crontab: print(f"Predict_X shape: {predict_X.shape}")

# %% [markdown]
# ## Make prediction

# %%
import tensorflow as tf
tf.keras.utils.disable_interactive_logging()

if not crontab: print("Num GPUs Available: ", len(tf.config.experimental.list_physical_devices('GPU')))

# %%
import os
import glob
from keras.models import load_model

# Get a list of all model files in the directory
model_files = glob.glob(os.path.join(newestDir, "*.keras"))

# Sort the list of model files by modification time (most recent first)
model_files.sort(key=os.path.getmtime, reverse=True)

# Select the most recent model file
most_recent_model_file = model_files[0]

# Load the selected model
model = load_model(most_recent_model_file, )

# Print the path of the loaded model for verification
if not crontab: print("Loaded model:", most_recent_model_file)


# Make predictions
predict_result = model.predict(predict_X)

# Invert predictions to original scale
inverted_predictions = scaler.inverse_transform(predict_result)


# %%
# Print or use the predictions as needed
if not crontab: print(f"Predictions: {inverted_predictions[0][0]}")

# %% [markdown]
# ## Add VCData back

# %%
def total_demand(row):
    # print(row)
    date_obj = dt.datetime.strptime(str(row['Date']), '%Y-%m-%d')
    year = date_obj.year
    quarter = (date_obj.month - 1) // 3 + 1
    
    period = row['Period']
    
    isWeekend = 1 if date_obj.isoweekday() > 5 else 0
    isPublicHoliday = date_obj in sg_holidays
    
    if isWeekend or isPublicHoliday:
        # print(f"Date: {date_obj} isWeekend: {isWeekend} isPublicHoliday: {isPublicHoliday}")
        tcq = vc_per[(vc_per['Year'] == year) & (vc_per['Quarter'] == quarter) & (vc_per['Period'] == period)]['TCQ_Weekday'].values[0] / 1000
    else:
        tcq = vc_per[(vc_per['Year'] == year) & (vc_per['Quarter'] == quarter) & (vc_per['Period'] == period)]['TCQ_Weekend_PH'].values[0] / 1000

    demand = tcq + row["Predicted_Demand"]
    return demand

# %%
data = {
    "Date": [next_date],
    "Period": [next_period],
    "Predicted_Demand": [inverted_predictions[0][0]]
}

data = pd.DataFrame(data)

# %%
data["Predicted_Demand"] = data.apply(lambda row: total_demand(row), axis=1)
predicted_demand = data["Predicted_Demand"][0]
print(f"# Predicted Demand: {predicted_demand}")

# %% [markdown]
# ## Save prediction to DB

# %%
from sqlalchemy import text

# Check if the table 'Predicted_Demand' exists
table_exists = engine.dialect.has_table(conn, 'DemandForecast')
if not crontab: print(f"Table 'DemandForecast' exists: {table_exists}")

if not table_exists:
    # Create the table 'DemandForecast'
    create_table_query = """
    CREATE TABLE public."DemandForecast" (
        "Date" DATE,
        "Period" INTEGER,
        "Predicted_Demand" FLOAT,
        PRIMARY KEY ("Date", "Period")
    )
    """
    conn.execute(text(create_table_query))

# Check if a row with the same Date and Period exists
row_exists_query = f"""
SELECT EXISTS (
    SELECT 1
    FROM public."DemandForecast"
    WHERE "Date" = '{next_date}' AND "Period" = '{next_period}'
)
"""
row_exists = conn.execute(text(row_exists_query)).scalar()
if not crontab: print(f"Row exists: {row_exists}")

if row_exists:
    # Update the existing row with the predicted net demand
    update_query = f"""
    UPDATE public."DemandForecast"
    SET "Predicted_Demand" = {predicted_demand}
    WHERE "Date" = '{next_date}' AND "Period" = {next_period}
    """
    conn.execute(text(update_query))
else:
    # Insert a new row with the predicted net demand
    insert_query = f"""
    INSERT INTO public."DemandForecast" ("Date", "Period", "Predicted_Demand")
    VALUES ('{next_date}', {next_period}, {predicted_demand})
    """
    conn.execute(text(insert_query))

# %%
conn.commit()
conn.close()

# %%



