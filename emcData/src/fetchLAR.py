# %% [markdown]
# # NEMS Data fetching to Database

# %%
crontab = False
ROOT = '/home/sdc/emcData'

# %%
from dep import nemsData2 as nems
import pandas as pd
import urllib3
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# %%
# Load the environment variables from the .env file
env_file = f'{ROOT}/.env' if crontab else '.env'
load_dotenv(env_file)

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

# %%
def getCorpProcessed(startDate_str):
    
    corp_df = nems.getCorp(startDate_str)

    corp_df = corp_df[['Date', 'Period'] +
                      [col for col in corp_df.columns if col not in ['Date', 'Period', 'reportType', 'secondaryReserve','Demand', 'TCL', 'USEP', 'LCP','EHEUR', 'Solar']]]

    # Convert 'period' to int
    corp_df['Period'] = corp_df['Period'].astype(int)

    # Convert 'regulation', 'primaryReserve', 'contingencyReserve' to float
    float_cols = ['Regulation', 'PrimaryReserve','ContingencyReserve']
    corp_df[float_cols] = corp_df[float_cols].apply(pd.to_numeric, errors='coerce')
    
    return corp_df

def getMCR010(mcrSerie):
    mcr010 = nems.getMCRReport('MCR010', mcrSerie.iloc[0])
    mcr010_df = mcr010[['ForecastDate', 'ForecastPeriod', 'TotalLoad', 'TCL', 'USEP', 'CUSEP', 'LCP',
                        'TransmissionLoss', 'EnergyShortfall', 'RLQ','EHEUR','Solar']].copy()
    
    mcr010_df['ForecastPeriod'] = mcr010_df['ForecastPeriod'].astype('int64')

    mcr010_df['Date'] = mcr010_df['ForecastDate']
    mcr010_df['Period'] = mcr010_df['ForecastPeriod'].astype('int64')
    
    mcr010_df['LoadScenario'] = mcrSerie['LoadScenario'].values[0]
    


    def processForecastTime (row):
        if row['ForecastPeriod'] - 1 > 0:
            row['ForecastPeriod'] = row['ForecastPeriod'] - 1
            return row
        else: 
            row['ForecastPeriod'] = 48
            row['ForecastDate'] = row['ForecastDate'] - pd.Timedelta(days=1)
            return row
        
    mcr010_df = mcr010_df.apply(processForecastTime, axis=1)

    mcr010_df.rename(
        {
            'TotalLoad': 'Demand'
        },
        axis=1,
        inplace=True
    )

    
    return mcr010_df

def getMCR012(mcrSerie):
    mcr012_df = nems.getMCRReport('MCR012', mcrSerie.iloc[0])

    
    mcr012 = {}

    for index, row in mcr012_df.iterrows():
        mcr012["ForecastDate"] = [row["ForecastDate"]]
        mcr012["ForecastPeriod"] = [row["ForecastPeriod"]]
        mcr012[f"{row['AncillaryService']}_ReserveRequirement"] = [row["ReserveRequirementMW"]]
        mcr012[f"{row['AncillaryService']}_RegulationShortfall"] = [row["RegulationShortfallMW"]]
    mcr012_df = pd.DataFrame(mcr012)
    
    
    mcr012_df.rename(
        {   'ForecastDate':'Date',
            'ForecastPeriod':'Period',
            'REGULATION_ReserveRequirement': 'RegulationRequirement',
            'REGULATION_RegulationShortfall': 'RegulationShortfall',
            'PRIMARY RESERVE_ReserveRequirement': 'PrimaryReserveRequirement',
            'PRIMARY RESERVE_RegulationShortfall': 'PrimaryReserveShortfall',
            'CONTINGENCY RESERVE_ReserveRequirement': 'ContingencyReserveRequirement',
            'CONTINGENCY RESERVE_RegulationShortfall': 'ContingencyReserveShortfall'
        },
        axis=1,
        inplace=True
    )
    mcr012_df['Period'] = mcr012_df['Period'].astype('int64')
    
    return mcr012_df

# %%
def getLarDf(corp_df_day, mcr010_df, mcr012_df):
    mcr_df = pd.merge(mcr010_df, mcr012_df, how='inner',
                          on=['Date', 'Period'])


    lar_df = pd.merge(corp_df_day, mcr_df, how='inner',
                        on=['Date', 'Period'])


    float64_cols = ['Demand', 'TCL', 'USEP', 'CUSEP', 'LCP', 'TransmissionLoss',
                    'EnergyShortfall', 'RLQ', 'EHEUR', 'RegulationRequirement','Regulation', 'RegulationShortfall',
                    'PrimaryReserveRequirement', 'PrimaryReserve', 'PrimaryReserveShortfall',
                    'ContingencyReserveRequirement', 'ContingencyReserve', 'ContingencyReserveShortfall', 'Solar']
        
    lar_df[float64_cols] = lar_df[float64_cols].astype('float64')

    lar_df['Date'] = lar_df['Date'].dt.date
    lar_df['ForecastDate'] = lar_df['ForecastDate'].dt.date

    lar_df.fillna(0, inplace=True)

    lar_df = lar_df[['Date', 'Period', 'LoadScenario','ForecastDate', 'ForecastPeriod',
                    'Demand', 'TCL', 'USEP', 'CUSEP', 'LCP', 'TransmissionLoss',
                    'EnergyShortfall', 'RLQ', 'EHEUR', 'RegulationRequirement','Regulation', 'RegulationShortfall',
                    'PrimaryReserveRequirement', 'PrimaryReserve', 'PrimaryReserveShortfall',
                    'ContingencyReserveRequirement', 'ContingencyReserve', 'ContingencyReserveShortfall', 'Solar']]
    
    return lar_df

# %%
import time

def waitAndRetry (func, targetType, *args):

    for i in range(10):
        try:
            data = func(*args)
            
            if not type(data) == targetType:
                print(data)
                raise Exception(f"Expected {targetType} but got {type(data)}")
            
            return data
            
        except Exception as e:
            print(f"In API call, error message: {e}")
            time.sleep(30)
            continue


# %%
from datetime import datetime as dt
from datetime import timedelta as td

runDate = dt.strptime('01-Jul-2023', '%d-%b-%Y')
endDate = dt.strptime('03-Jul-2023', '%d-%b-%Y')
# endDate = dt.strptime('10-Apr-2024', '%d-%b-%Y')
delta = td(days=1)

print("Fetching data from ", runDate, " to ", endDate)

exist = 0
new = 0

while (runDate < endDate):

    runDate_str = runDate.strftime(format='%d-%b-%Y')
    # print(startDate_str)

    '''
    get Corp
    '''
    corp_df = waitAndRetry(getCorpProcessed, pd.DataFrame, runDate_str)
    # print("Corp get.")


    '''
    get MCR001
    '''
    mcrDf = waitAndRetry(nems.getMCR001, pd.DataFrame, runDate_str, 'M')
    # print("MCR001 get.")

    

    '''
    get MCR010
    get MCR012
    combine DPR
    insert DB
    '''
    for period in range(1, 49, 1):

        # Extract data of this 'Date' 'Period'
         # Extract data of this 'Date' 'Period'
        corp_df_day = corp_df[(corp_df['Date'] == runDate) & (
            corp_df['Period'] == period)].copy()

        mcrSerie = mcrDf[(mcrDf['FirstDate'] == runDate_str) & (
            mcrDf['FirstPeriod'] == str(period))].copy()

        # get MCR010
        mcr010_df = waitAndRetry(getMCR010, pd.DataFrame, mcrSerie)
        # print("MCR010 get.")

        # get MCR012
        mcr012_df = waitAndRetry(getMCR012, pd.DataFrame, mcrSerie)
        # print("MCR012 get.")

        # combine
        lar_df = getLarDf(corp_df_day, mcr010_df, mcr012_df)
        # print("LAR combined.")

        
        
         # check existing row
        row_exists_query = f"""
            SELECT 1 FROM emcdata."RealTimeLAR"
                WHERE "Date" = '{runDate_str}' AND "Period" = '{period}';
            """
        row_exists = conn.execute(text(row_exists_query)).scalar()

        if row_exists:

            lar_se = lar_df.iloc[0]

            update_query = '''
                UPDATE emcdata."RealTimeLAR"
                    SET "Date"={}, "Period"={}, "LoadScenario"={}, "ForecastDate"={}, "ForecastPeriod"={}, "Demand"={}, "TCL"={}, "USEP"={}, "CUSEP"={}, "LCP"={}, "TransmissionLoss"={}, "EnergyShortfall"={}, "RLQ"={}, "EHEUR"={}, "RegulationRequirement"={}, "Regulation"={}, "RegulationShortfall"={}, "PrimaryReserveRequirement"={}, "PrimaryReserve"={}, "PrimaryReserveShortfall"={}, "ContingencyReserveRequirement"={}, "ContingencyReserve"={}, "ContingencyReserveShortfall"={}, "Solar"={}
                    WHERE "Date" = {} AND "Period" = {};
            '''.format(
                f"'{runDate_str}'",
                lar_se['Period'],
                f"'{lar_se['LoadScenario']}'",
                f"'{lar_se['ForecastDate']}'",
                lar_se['ForecastPeriod'],
                lar_se['Demand'],
                lar_se['TCL'],
                lar_se['USEP'],
                lar_se['CUSEP'],
                lar_se['LCP'],
                lar_se['TransmissionLoss'],
                lar_se['EnergyShortfall'],
                lar_se['RLQ'],
                lar_se['EHEUR'],
                lar_se['RegulationRequirement'],
                lar_se['Regulation'],
                lar_se['RegulationShortfall'],
                lar_se['PrimaryReserveRequirement'],
                lar_se['PrimaryReserve'],
                lar_se['PrimaryReserveShortfall'],
                lar_se['ContingencyReserveRequirement'],
                lar_se['ContingencyReserve'],
                lar_se['ContingencyReserveShortfall'],
                lar_se['Solar'],
                f"'{runDate_str}'",
                period
            )

            # print(update_query)


            conn.execute(text(update_query))
            exist += 1
            # print("DPR updated. [exist]")
        else:
            lar_df.to_sql('RealTimeLAR', conn,
                          if_exists='append', index=False)
            new += 1
            # print("DPR inserted. [new]")

        # break  # Comment this if can repeatedly insert into DB

       

    runDate = runDate + delta
    print("Date: ", runDate_str, " done. Sleeping for 10s ...")
    time.sleep(20)
    conn.commit()
    # break  # Comment this if can repeatedly insert into DB

# %%
conn.commit()
conn.close()

# %%
print("Done.")
print("Updated rows: ", exist)
print("New rows: ", new)

# %%



