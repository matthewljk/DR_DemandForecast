# %% [markdown]
# # Scheduled NEMS Data fetching to Database

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

# %% [markdown]
# ## Methods

# %% [markdown]
# ### Corp Data Methods

# %%
def corp_for_dpr(corp_df):
    corp_df = corp_df.copy()
    corp_df = corp_df[['Date', 'Period'] +
                      [col for col in corp_df.columns if col not in ['Date', 'Period', 'reportType', 'secondaryReserve']]]

    # Convert to int
    corp_df['Period'] = corp_df['Period'].astype(int)

    # Convert to float
    float_cols = ['Demand', 'TCL', 'USEP', 'LCP', 'Regulation', 'PrimaryReserve']
    corp_df[float_cols] = corp_df[float_cols].apply(pd.to_numeric, errors='coerce')

    # If 'secondaryReserve' and 'contingencyReserve' contain 'None', convert to float and keep NaN
    corp_df[['ContingencyReserve', 'EHEUR', 'Solar']] = corp_df[[
        'ContingencyReserve', 'EHEUR', 'Solar']].apply(pd.to_numeric, errors='coerce')
    
    return corp_df


def corp_for_lar(corp_df):
    corp_df = corp_df.copy()
    corp_df = corp_df[['Date', 'Period'] +
                      [col for col in corp_df.columns if col not in ['Date', 'Period', 'reportType', 'secondaryReserve', 'Demand', 'TCL', 'USEP', 'LCP', 'EHEUR', 'Solar']]]

    # Convert 'period' to int
    corp_df['Period'] = corp_df['Period'].astype(int)

    # Convert 'regulation', 'primaryReserve', 'contingencyReserve' to float
    float_cols = ['Regulation', 'PrimaryReserve', 'ContingencyReserve']
    corp_df[float_cols] = corp_df[float_cols].apply(
        pd.to_numeric, errors='coerce')

    return corp_df

# %% [markdown]
# ### MCR010 Data Methods

# %%
def get_mcr010(mcr_serie):
    mcr010 = nems.getMCRReport('MCR010', mcr_serie.iloc[0])
    return mcr010

# %%
def mcr010_for_dpr(mcr010_df):   
    mcr010_df = mcr010_df.copy() 
    mcr010_df = mcr010_df[['ForecastDate', 'ForecastPeriod', 'CUSEP', 'TransmissionLoss', 'EnergyShortfall', 'RLQ']].copy()
    mcr010_df['ForecastPeriod'] = mcr010_df['ForecastPeriod'].astype(int)
    
    return mcr010_df

# %%
def mcr010_for_lar(mcr010_df, mcr_serie):
    mcr010_df = mcr010_df.copy() 
    mcr010_df = mcr010_df[['ForecastDate', 'ForecastPeriod', 'TotalLoad', 'TCL', 'USEP', 'CUSEP', 'LCP', 'TransmissionLoss', 'EnergyShortfall', 'RLQ', 'EHEUR', 'Solar']].copy()

    mcr010_df['ForecastPeriod'] = mcr010_df['ForecastPeriod'].astype('int64')
    mcr010_df['Period'] = mcr010_df['ForecastPeriod'].astype('int64')

    mcr010_df['Date'] = mcr010_df['ForecastDate']
    mcr010_df['LoadScenario'] = mcr_serie['LoadScenario'].values[0]

    def processForecastTime(row):
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

# %% [markdown]
# ### MR012 Data Methods

# %%
def get_mcr012(mcrSerie):
    mcr012_df = nems.getMCRReport('MCR012', mcrSerie.iloc[0])
    return mcr012_df

# %%
def mcr012_for_dpr(mcr012_df):
    
    mcr012_df = mcr012_df.copy()
    
    # Flatten MCR012
    mcr012_data = {}
    for index, row in mcr012_df.iterrows():
        mcr012_data["ForecastDate"] = [row["ForecastDate"]]
        mcr012_data["ForecastPeriod"] = [row["ForecastPeriod"]]
        mcr012_data[f"{row['AncillaryService']}_ReserveRequirement"] = [row["ReserveRequirementMW"]]
        mcr012_data[f"{row['AncillaryService']}_RegulationShortfall"] = [row["RegulationShortfallMW"]]
    
    # Create a new MCR012 DataFrame
    mcr012_df = pd.DataFrame(mcr012_data)
    
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
def mcr012_for_lar(mcr012_df):
    mcr012_df = mcr012_df.copy()
    
    # Flatten MCR012
    mcr012_data = {
        'Date': [],
        'Period': [],
        'RegulationRequirement': [],
        'RegulationShortfall': [],
        'PrimaryReserveRequirement': [],
        'PrimaryReserveShortfall': [],
        'ContingencyReserveRequirement': [],
        'ContingencyReserveShortfall': [],
    }
    
    count = 1
    for index, row in mcr012_df.iterrows():

        if count%3 == 0:
            mcr012_data['Date'].append(row["ForecastDate"])
            mcr012_data['Period'].append(row["ForecastPeriod"])
            count = 1
        else: 
            count += 1

        if row['AncillaryService'] == 'REGULATION':
            mcr012_data['RegulationRequirement'].append(row["ReserveRequirementMW"])
            mcr012_data['RegulationShortfall'].append(row["RegulationShortfallMW"])
        if row['AncillaryService'] == 'PRIMARY RESERVE':
            mcr012_data['PrimaryReserveRequirement'].append(row["ReserveRequirementMW"])
            mcr012_data['PrimaryReserveShortfall'].append(row["RegulationShortfallMW"])
        if row['AncillaryService'] == 'CONTINGENCY RESERVE':
            mcr012_data['ContingencyReserveRequirement'].append(row["ReserveRequirementMW"])
            mcr012_data['ContingencyReserveShortfall'].append(row["RegulationShortfallMW"])
    

    # Create a new MCR012 DataFrame
    mcr012_df = pd.DataFrame(mcr012_data)
    mcr012_df['Period'] = mcr012_df['Period'].astype('int64')

    return mcr012_df

# %% [markdown]
# ### Final Data Methods

# %%
def get_dpr(corp_peri_df, mcr010_df, mcr012_df):
    
    mcr_df = pd.merge(mcr010_df, mcr012_df, how='inner',
                      on=['ForecastDate', 'ForecastPeriod'])
    
    mcr_df.rename({
        'ForecastDate': 'Date',
        'ForecastPeriod': 'Period'
        }, 
        axis=1, 
        inplace=True
    )

    dpr_df = pd.merge(corp_peri_df, mcr_df, how='inner',
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
                    'Demand', 'TCL', 'USEP', 'CUSEP',  'LCP', 'TransmissionLoss', 'EnergyShortfall',
                    'RLQ', 'Regulation', 'RegulationRequirement', 'RegulationShortfall',
                    'PrimaryReserve', 'PrimaryReserveRequirement', 'PrimaryReserveShortfall',
                    'ContingencyReserve', 'ContingencyReserveRequirement', 
                    'ContingencyReserveShortfall', 'EHEUR', 'Solar'
                    ]]
    
    return dpr_df

# %%
def get_lar(corp_peri_df, mcr010_df, mcr012_df):
    
    mcr_df = pd.merge(mcr010_df, mcr012_df, how='inner',
                      on=['Date', 'Period'])

    lar_df = pd.merge(corp_peri_df, mcr_df, how='inner',
                      on=['Date', 'Period'])

    float64_cols = ['Demand', 'TCL', 'USEP', 'CUSEP', 'LCP', 'TransmissionLoss',
                    'EnergyShortfall', 'RLQ', 'EHEUR', 'RegulationRequirement', 'Regulation', 
                    'RegulationShortfall', 'PrimaryReserveRequirement', 'PrimaryReserve', 
                    'PrimaryReserveShortfall', 'ContingencyReserveRequirement', 
                    'ContingencyReserve', 'ContingencyReserveShortfall', 'Solar']

    lar_df[float64_cols] = lar_df[float64_cols].astype('float64')

    lar_df['Date'] = lar_df['Date'].dt.date
    lar_df['ForecastDate'] = lar_df['ForecastDate'].dt.date

    lar_df.fillna(0, inplace=True)

    lar_df = lar_df[['Date', 'Period', 'LoadScenario', 'ForecastDate', 'ForecastPeriod',
                    'Demand', 'TCL', 'USEP', 'CUSEP', 'LCP', 'TransmissionLoss',
                     'EnergyShortfall', 'RLQ', 'EHEUR', 'RegulationRequirement', 'Regulation', 
                     'RegulationShortfall', 'PrimaryReserveRequirement', 'PrimaryReserve', 
                     'PrimaryReserveShortfall', 'ContingencyReserveRequirement', 
                     'ContingencyReserve', 'ContingencyReserveShortfall', 'Solar'
                    ]]

    return lar_df

# %% [markdown]
# ### Network Traffic Control 

# %%
import time as t

def wait_retry (func, targetType=pd.DataFrame):

    for i in range(10):
        try:
            data = func
            
            if not type(data) == targetType:
                print(data)
                raise Exception(f"Expected {targetType} but got {type(data)}")
            
            print('.', end='')
            t.sleep(5) # Sleep awhile if successfully fetched data
            return data
            
        except Exception as e:
            print(f"In API call, error message: {e}")
            t.sleep(30) # Sleep for a long time to wait for next try
            continue


# %% [markdown]
# ## Main Process

# %% [markdown]
# ### NEMS Requests

# %%
import datetime as dt
import pytz

DATADIR = '/home/sdc/emcData/data/'

now = dt.datetime.now(pytz.timezone('Asia/Singapore'))

run_date = now.date()
run_date_str = run_date.strftime(format='%d-%b-%Y')

time = now.strftime("%H:%M")
period = int(now.strftime("%H")) * 2 + int(now.strftime("%M")) // 30 + 1


# %%

print(f"Fetching {run_date_str} Period {period}", end=" ")

''' Get Corp for DPR and LAR respectively '''
print(" Corp", end="")
corp_df = wait_retry(nems.getCorp(run_date_str))
# corp_df.to_csv(f'{DATADIR}Corp_{run_date_str}_{period}.csv', index=False)
corp_dpr = corp_for_dpr(corp_df)
corp_lar = corp_for_lar(corp_df)


''' Get MCR001'''
print(" MCR001", end="")
# DPR
mcr_df = wait_retry(nems.getMCR001(run_date_str, 'M'))
# mcr_df.to_csv(f'{DATADIR}MCR001_DPR_{run_date_str}_{period}.csv', index=False)
mcr_serie = mcr_df[
    (mcr_df['FirstDate'] == run_date_str) & 
    (mcr_df['FirstPeriod'] == str(period))].copy()

# LAR H
mcr_h_df = wait_retry(nems.getMCR001(run_date_str, 'H', runType='LAR'))
# mcr_h_df.to_csv(f'{DATADIR}MCR001_LAR_H_{run_date_str}_{period}.csv', index=False)
mcr_h_serie = mcr_h_df[
    (mcr_h_df['FirstDate'] == run_date_str) & 
    (mcr_h_df['FirstPeriod'] == str(period+1))].copy()

# LAR M
mcr_m_df = wait_retry(nems.getMCR001(run_date_str, 'M', runType='LAR'))
# mcr_m_df.to_csv(f'{DATADIR}MCR001_LAR_M_{run_date_str}_{period}.csv', index=False)
mcr_m_serie = mcr_m_df[
    (mcr_m_df['FirstDate'] == run_date_str) & 
    (mcr_m_df['FirstPeriod'] == str(period+1))].copy()

# LAR L
mcr_l_df = wait_retry(nems.getMCR001(run_date_str, 'L', runType='LAR'))
# mcr_l_df.to_csv(f'{DATADIR}MCR001_LAR_l_{run_date_str}_{period}.csv', index=False)
mcr_l_serie = mcr_l_df[
    (mcr_l_df['FirstDate'] == run_date_str) & 
    (mcr_l_df['FirstPeriod'] == str(period+1))].copy()



''' Get MCR010 '''
print(" MCR010", end="")
# DPR
mcr010_df = wait_retry(get_mcr010(mcr_serie))
# mcr010_df.to_csv(f'{DATADIR}MCR010_DPR_{run_date_str}_{period}.csv', index=False)
# LAR
mcr010_h_df = wait_retry(get_mcr010(mcr_h_serie))
# mcr010_h_df.to_csv(f'{DATADIR}MCR010_LAR_H_{run_date_str}_{period}.csv', index=False)
mcr010_m_df = wait_retry(get_mcr010(mcr_m_serie))
# mcr010_m_df.to_csv(f'{DATADIR}MCR010_LAR_M_{run_date_str}_{period}.csv', index=False)
mcr010_l_df = wait_retry(get_mcr010(mcr_l_serie))
# mcr010_l_df.to_csv(f'{DATADIR}MCR010_LAR_L_{run_date_str}_{period}.csv', index=False)


''' Get MCR012 '''
print(" MCR012", end="")
# DPR
mcr012_df = wait_retry(get_mcr012(mcr_serie))
# mcr012_df.to_csv(f'{DATADIR}MCR012_DPR_{run_date_str}_{period}.csv', index=False)
# LAR
mcr012_h_df = wait_retry(get_mcr012(mcr_h_serie))
# mcr012_h_df.to_csv(f'{DATADIR}MCR012_LAR_H_{run_date_str}_{period}.csv', index=False)
mcr012_m_df = wait_retry(get_mcr012(mcr_m_serie))
# mcr012_m_df.to_csv(f'{DATADIR}MCR012_LAR_M_{run_date_str}_{period}.csv', index=False)
mcr012_l_df = wait_retry(get_mcr012(mcr_l_serie))
# mcr012_l_df.to_csv(f'{DATADIR}MCR012_LAR_L_{run_date_str}_{period}.csv', index=False)


# %% [markdown]
# ### Data Construction & DB Actions

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
def db_dpr(run_date_str, period, dpr_df):
    
    # check existing row
    row_exists_query = f"""
        SELECT 1 FROM emcdata."RealTimeDPR"
            WHERE "Date" = '{run_date_str}' AND "Period" = '{period}';
        """
    row_exists = conn.execute(text(row_exists_query)).scalar()

    if row_exists:

        dpr_se = dpr_df.iloc[0]

        update_query = '''
            UPDATE emcdata."RealTimeDPR"
                SET "Date"={}, "Period"={}, "Demand"={}, "TCL"={}, "USEP"={}, "CUSEP"={}, "LCP"={}, "TransmissionLoss"={}, "EnergyShortfall"={}, "RLQ"={}, "Regulation"={}, "RegulationRequirement"={}, "RegulationShortfall"={}, "PrimaryReserve"={}, "PrimaryReserveRequirement"={}, "PrimaryReserveShortfall"={}, "ContingencyReserve"={}, "ContingencyReserveRequirement"={}, "ContingencyReserveShortfall"={}, "EHEUR"={}, "Solar"={}
                WHERE "Date" = {} AND "Period" = {};
        '''.format(
            f"'{run_date_str}'",
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
            f"'{run_date_str}'",
            period
        )

        conn.execute(text(update_query))
        conn.commit()
        print("*", end="")
        return 0
    else:
        dpr_df.to_sql('RealTimeDPR', conn, if_exists='append', index=False)
        conn.commit()
        print(".", end="")
        return 1

# %%
def db_lar(lar_df):
    lar_se = lar_df.iloc[0]

    run_date_str = lar_se['Date'].strftime(format="%Y-%m-%d")
    period = lar_se['Period']

    # check existing row
    row_exists_query = f"""
        SELECT 1 FROM emcdata."RealTimeLAR"
            WHERE 
                "Date"='{run_date_str}' AND 
                "Period"={period} AND 
                "LoadScenario"='{lar_se['LoadScenario']}';
        """
    row_exists = conn.execute(text(row_exists_query)).scalar()

    if row_exists:


        update_query = '''
            UPDATE emcdata."RealTimeLAR"
                SET "Date"={}, "Period"={}, "LoadScenario"={}, "ForecastDate"={}, "ForecastPeriod"={}, "Demand"={}, "TCL"={}, "USEP"={}, "CUSEP"={}, "LCP"={}, "TransmissionLoss"={}, "EnergyShortfall"={}, "RLQ"={}, "EHEUR"={}, "RegulationRequirement"={}, "Regulation"={}, "RegulationShortfall"={}, "PrimaryReserveRequirement"={}, "PrimaryReserve"={}, "PrimaryReserveShortfall"={}, "ContingencyReserveRequirement"={}, "ContingencyReserve"={}, "ContingencyReserveShortfall"={}, "Solar"={}
                WHERE "Date" = {} AND "Period" = {} AND "LoadScenario"={};
        '''.format(
            f"'{run_date_str}'",
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
            f"'{run_date_str}'",
            period,
            f"'{lar_se['LoadScenario']}'"
        )

        # print(update_query)

        conn.execute(text(update_query))
        conn.commit()
        print("*", end="")
        return 0
    else:
        lar_df.to_sql('RealTimeLAR', conn, if_exists='append', index=False)
        conn.commit()
        print(".", end="")
        return 1

# %%
exist = 0
new = 0

''' DPR '''
print("DPR", end='')
# DPR required data
corp_peri_df = corp_dpr[
    (corp_dpr['Date'] == run_date_str) & 
    (corp_dpr['Period'] == period)].copy()

mcr010_dpr_df = mcr010_for_dpr(mcr010_df)
mcr012_dpr_df = mcr012_for_dpr(mcr012_df)

# Construct DPR
dpr_df = get_dpr(corp_peri_df, mcr010_dpr_df, mcr012_dpr_df)

# DB actions
add_new = db_dpr(run_date_str, period, dpr_df)

if add_new: 
    new += 1
else: exist += 1

conn.commit()

# %%
print("LAR", end='')
# LAR required data
corp_peri_df = corp_lar[
    (corp_lar['Date'] == run_date_str) & 
    (corp_lar['Period'] == period+1)].copy()

''' LAR H '''
# Construct LAR
mcr010_lar_df = mcr010_for_lar(mcr010_h_df, mcr_h_serie)
mcr012_lar_df = mcr012_for_lar(mcr012_h_df)
lar_df = get_lar(corp_peri_df, mcr010_lar_df, mcr012_lar_df)

# DB actions
add_new = db_lar(lar_df)

if add_new:
    new += 1
else: 
    exist += 1


''' LAR M '''
# Construct LAR
mcr010_lar_df = mcr010_for_lar(mcr010_m_df, mcr_m_serie)
mcr012_lar_df = mcr012_for_lar(mcr012_m_df)
lar_df = get_lar(corp_peri_df, mcr010_lar_df, mcr012_lar_df)

# DB actions
add_new = db_lar(lar_df)

if add_new:
    new += 1
else: 
    exist += 1
    
''' LAR L '''
# Construct LAR
mcr010_lar_df = mcr010_for_lar(mcr010_l_df, mcr_l_serie)
mcr012_lar_df = mcr012_for_lar(mcr012_l_df)
lar_df = get_lar(corp_peri_df, mcr010_lar_df, mcr012_lar_df)

# DB actions
add_new = db_lar(lar_df)

if add_new:
    new += 1
else: 
    exist += 1


# %%
conn.commit()
conn.close()

# %%
print("All done!")
print(f"Updated {exist}rows. Added {new} rows.")

# %%



