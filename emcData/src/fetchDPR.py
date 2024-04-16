# %% [markdown]
# # NEMS DPR Data fetching to Database

# %%
from datetime import timedelta as td
from datetime import datetime as dt
import time
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
import urllib3
import pandas as pd
from dep import nemsData2 as nems
crontab = False
ROOT = '/home/sdc/emcData'

# %%

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
                      [col for col in corp_df.columns if col not in ['Date', 'Period', 'reportType', 'secondaryReserve']]]

    # Convert to int
    corp_df['Period'] = corp_df['Period'].astype(int)

    # Convert to float
    float_cols = ['Demand', 'TCL', 'USEP',
                  'LCP', 'Regulation', 'PrimaryReserve']
    corp_df[float_cols] = corp_df[float_cols].apply(
        pd.to_numeric, errors='coerce')

    # If 'secondaryReserve' and 'contingencyReserve' contain 'None', convert to float and keep NaN
    corp_df[['ContingencyReserve', 'EHEUR', 'Solar']] = corp_df[[
        'ContingencyReserve', 'EHEUR', 'Solar']].apply(pd.to_numeric, errors='coerce')

    return corp_df


def getMCR010(mcrSerie):
    mcr010 = nems.getMCRReport('MCR010', mcrSerie.iloc[0])

    mcr010_df = mcr010[['ForecastDate', 'ForecastPeriod', 'CUSEP',
                        'TransmissionLoss', 'EnergyShortfall', 'RLQ']].copy()
    mcr010_df['ForecastPeriod'] = mcr010_df['ForecastPeriod'].astype(int)

    return mcr010_df


def getMCR012(mcrSerie):
    mcr012_df = nems.getMCRReport('MCR012', mcrSerie.iloc[0])

    mcr012 = {}
    for index, row in mcr012_df.iterrows():
        mcr012["ForecastDate"] = [row["ForecastDate"]]
        mcr012["ForecastPeriod"] = [row["ForecastPeriod"]]
        mcr012[f"{row['AncillaryService']}_ReserveRequirement"] = [
            row["ReserveRequirementMW"]]
        mcr012[f"{row['AncillaryService']}_RegulationShortfall"] = [
            row["RegulationShortfallMW"]]
    mcr012_df = pd.DataFrame(mcr012)

    mcr012_df.rename(
        {
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
    mcr012_df['ForecastPeriod'] = mcr012_df['ForecastPeriod'].astype(int)

    return mcr012_df

# %%


def getDprDf(corp_df_day, mcr010_df, mcr012_df):
    mcr_df = pd.merge(mcr010_df, mcr012_df, how='inner',
                      on=['ForecastDate', 'ForecastPeriod'])

    mcr_df.rename({
        'ForecastDate': 'Date',
        'ForecastPeriod': 'Period'
    },
        axis=1,
        inplace=True
    )

    dpr_df = pd.merge(corp_df_day, mcr_df, how='inner',
                      on=['Date', 'Period'])

    float64_cols = ['Demand', 'TCL', 'USEP', 'LCP', 'Regulation',
                    'PrimaryReserve', 'ContingencyReserve', 'EHEUR', 'Solar', 'CUSEP',
                    'TransmissionLoss', 'EnergyShortfall', 'RLQ', 'RegulationRequirement',
                    'RegulationShortfall', 'PrimaryReserveRequirement',
                    'PrimaryReserveShortfall', 'ContingencyReserveRequirement',
                    'ContingencyReserveShortfall']
    dpr_df[float64_cols] = dpr_df[float64_cols].astype('float64')

    dpr_df['Date'] = dpr_df['Date'].dt.date

    dpr_df.fillna(0, inplace=True)

    dpr_df = dpr_df[['Date', 'Period',
                    'Demand', 'TCL', 'USEP', 'CUSEP',  'LCP', 'TransmissionLoss', 'EnergyShortfall', 'RLQ',
                     'Regulation', 'RegulationRequirement', 'RegulationShortfall',
                     'PrimaryReserve', 'PrimaryReserveRequirement', 'PrimaryReserveShortfall',
                     'ContingencyReserve', 'ContingencyReserveRequirement', 'ContingencyReserveShortfall', 'EHEUR', 'Solar'
                     ]]

    return dpr_df


# %%


def waitAndRetry(func, targetType, *args):

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

runDate = dt.strptime('05-Aug-2023', '%d-%b-%Y')
endDate = dt.strptime('10-Apr-2024', '%d-%b-%Y')
# endDate = dt.strptime('10-Apr-2024', '%d-%b-%Y')
delta = td(days=1)

print("Fetching data from ", runDate, " to ", endDate)

exist = 0
new = 0

while (runDate < endDate):

    runDate_str = runDate.strftime(format='%d-%b-%Y')
    # print(runDate_str)

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
        dpr_df = getDprDf(corp_df_day, mcr010_df, mcr012_df)
        # print("DPR combined.")

        # check existing row
        row_exists_query = f"""
            SELECT 1 FROM emcdata."RealTimeDPR"
                WHERE "Date" = '{runDate_str}' AND "Period" = '{period}';
            """
        row_exists = conn.execute(text(row_exists_query)).scalar()

        if row_exists:

            dpr_se = dpr_df.iloc[0]

            update_query = '''
                UPDATE emcdata."RealTimeDPR"
                    SET "Date"={}, "Period"={}, "Demand"={}, "TCL"={}, "USEP"={}, "CUSEP"={}, "LCP"={}, "TransmissionLoss"={}, "EnergyShortfall"={}, "RLQ"={}, "Regulation"={}, "RegulationRequirement"={}, "RegulationShortfall"={}, "PrimaryReserve"={}, "PrimaryReserveRequirement"={}, "PrimaryReserveShortfall"={}, "ContingencyReserve"={}, "ContingencyReserveRequirement"={}, "ContingencyReserveShortfall"={}, "EHEUR"={}, "Solar"={}
	                WHERE "Date" = {} AND "Period" = {};
            '''.format(
                f"'{runDate_str}'",
                dpr_se['Period'],
                dpr_se['Demand'],
                dpr_se['TCL'],
                dpr_se['USEP'],
                dpr_se['CUSEP'],
                dpr_se['LCP'],
                dpr_se['TransmissionLoss'],
                dpr_se['EnergyShortfall'],
                dpr_se['RLQ'],
                dpr_se['Regulation'],
                dpr_se['RegulationRequirement'],
                dpr_se['RegulationShortfall'],
                dpr_se['PrimaryReserve'],
                dpr_se['PrimaryReserveRequirement'],
                dpr_se['PrimaryReserveShortfall'],
                dpr_se['ContingencyReserve'],
                dpr_se['ContingencyReserveRequirement'],
                dpr_se['ContingencyReserveShortfall'],
                dpr_se['EHEUR'],
                dpr_se['Solar'],
                f"'{runDate_str}'",
                period
            )

            # print(update_query)

            conn.execute(text(update_query))
            exist += 1
            # print("DPR updated. [exist]")
        else:
            dpr_df.to_sql('RealTimeDPR', conn,
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
