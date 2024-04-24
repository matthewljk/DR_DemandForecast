# %% [markdown]
# # Scheduled NEMS Data fetching to Database

# %%
import os
from os.path import join

ROOT = '/home/sdc/DR_DemandForecast/emcData'

# %%
from dep import nemsData2 as nems
import pandas as pd
import urllib3
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# %%
import datetime as dt
from datetime import timedelta as delta
import pytz

DATADIR = '/home/sdc/DR_DemandForecast/emcData/data/'
CRONTAB = True

if CRONTAB:
    now = dt.datetime.now(pytz.timezone('Asia/Singapore'))
    
else:
    # Test 
    
    # now = dt.datetime(2024, 4, 16, 0, 5)   # Test period 1 (First period and the periods not involving tomorrow data)
    # now = dt.datetime(2024, 4, 22, 22, 55) # Test period 46 (The period involving tomorrow data)
    # now = dt.datetime(2024, 4, 21, 23, 40) # Test period 48 (Last period)
    
    # Manual override
    now = dt.datetime(2024, 4, 22, 18, 10) # Test period 48 (Last period)
    

date_today = now.date()
time_today = now.strftime("%H:%M")
period_now = int(now.strftime("%H")) * 2 + int(now.strftime("%M")) // 30 + 1

# If 12 periods after current one include periods of tomorrow.
# A.k.a Starts from period 37.
need_tomorrow_data = period_now >= 37
if need_tomorrow_data:
    date_tomorrow = date_today + delta(days=1)

# LAR specific start date and period
if period_now == 48:
    date_lar = date_today + dt.timedelta(days=1)
    period_lar = 1
else:
    date_lar = date_today
    period_lar = period_now + 1

if not CRONTAB:
    print(date_today.strftime(format='%d-%b-%Y'), period_now)
    print(date_lar.strftime(format='%d-%b-%Y'), period_lar)

# %%
print(f"[S][{date_today.strftime(format='%d-%b-%Y')} {now.time().strftime(format='%H:%M')} P-{period_now:0>2d}]", end="")

# %% [markdown]
# ## Fetch Data

# %% [markdown]
# ### Methods

# %% [markdown]
# #### Corp Data

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
# #### MCR010 Data

# %%
def get_mcr010(mcr_serie):
    mcr010 = nems.getMCRReport('MCR010', mcr_serie.iloc[0])
    return mcr010

# %% [markdown]
# #### MCR012 Data

# %%
def get_mcr012(mcrSerie):
    mcr012_df = nems.getMCRReport('MCR012', mcrSerie.iloc[0])
    return mcr012_df

# %% [markdown]
# #### Network Traffic Control 

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
            t.sleep(10) # Sleep awhile if successfully fetched data
            return data
            
        except Exception as e:
            print(f"In API call, error message: {e}")
            t.sleep(30) # Sleep for a long time to wait for next try
            continue


# %% [markdown]
# ### Main Process

# %% [markdown]
# #### Corp Data

# %%
print(" Corp", end="")
corp_df = wait_retry(nems.getCorp(date_today.strftime(format='%d-%b-%Y')))
if not CRONTAB: 
    corp_df.to_csv(f"{DATADIR}Corp_{date_today.strftime(format='%Y-%m-%d')}.csv", index=False)
corp_today = corp_for_dpr(corp_df)

if need_tomorrow_data:
    corp_df = wait_retry(nems.getCorp(date_tomorrow.strftime(format='%d-%b-%Y')))
    if not CRONTAB: 
        corp_df.to_csv(f"{DATADIR}Corp_{date_tomorrow.strftime(format='%Y-%m-%d')}.csv", index=False)
    corp_tomorrow = corp_for_lar(corp_df)
else:
    print("_", end="")

del corp_df

# %% [markdown]
# #### MCR001
# 
# In **MCR001**, both *DPR* and *LAR* returns 48 rows. 
# - *DPR* requires the row of current period. 
# - *LAR* requires the row of next period.

# %%
print(" MCR001", end="")
# DPR
mcr_df = wait_retry(nems.getMCR001(date_today.strftime(format='%d-%b-%Y'), 'M'))
if not CRONTAB:
    mcr_df.to_csv(f"{DATADIR}MCR001_DPR_{date_today.strftime(format='%d-%b-%Y')}_{period_now}.csv", index=False)
mcr_serie = mcr_df[
    (mcr_df['FirstDate'] == date_today) & 
    (mcr_df['FirstPeriod'] == period_now)].copy()


# LAR H
mcr_h_df = wait_retry(nems.getMCR001(date_lar.strftime(format='%d-%b-%Y'), 'H', runType='LAR'))
if not CRONTAB:
    mcr_h_df.to_csv(
        f"{DATADIR}MCR001_LAR_H_{date_lar.strftime(format='%d-%b-%Y')}_{period_lar}.csv", index=False)
mcr_h_serie = mcr_h_df[
    (mcr_h_df['FirstDate'] == date_lar) & 
    (mcr_h_df['FirstPeriod'] == period_lar)].copy()

# LAR M
mcr_m_df = wait_retry(nems.getMCR001(date_lar.strftime(format='%d-%b-%Y'), 'M', runType='LAR'))
if not CRONTAB:
    mcr_m_df.to_csv(
        f"{DATADIR}MCR001_LAR_M_{date_lar.strftime(format='%d-%b-%Y')}_{period_lar}.csv", index=False)
mcr_m_serie = mcr_m_df[
    (mcr_m_df['FirstDate'] == date_lar) & 
    (mcr_m_df['FirstPeriod'] == period_lar)].copy()

# LAR L
mcr_l_df = wait_retry(nems.getMCR001(date_lar.strftime(format='%d-%b-%Y'), 'L', runType='LAR'))
if not CRONTAB:
    mcr_l_df.to_csv(
        f"{DATADIR}MCR001_LAR_L_{date_lar.strftime(format='%d-%b-%Y')}_{period_lar}.csv", index=False)
mcr_l_serie = mcr_l_df[
    (mcr_l_df['FirstDate'] == date_lar) & 
    (mcr_l_df['FirstPeriod'] == period_lar)].copy()

# %% [markdown]
# #### MCR010
# 
# For **MCR010**, 
# 
# - *DPR* verison contains only 1 row of current period.
# - *LAR* version contains 12 rows ahead curren period.
# 

# %%
''' Get MCR010 '''
print(" MCR010", end="")
# DPR
mcr010_df = wait_retry(get_mcr010(mcr_serie))
if not CRONTAB:
    mcr010_df.to_csv(f"{DATADIR}MCR010_DPR_{date_today.strftime(format='%d-%b-%Y')}_{period_now}.csv", index=False)
# LAR
mcr010_h_df = wait_retry(get_mcr010(mcr_h_serie))
if not CRONTAB:
    mcr010_h_df.to_csv(f"{DATADIR}MCR010_LAR_H_{date_today.strftime(format='%d-%b-%Y')}_{period_lar}.csv", index=False)
mcr010_m_df = wait_retry(get_mcr010(mcr_m_serie))
if not CRONTAB:
    mcr010_m_df.to_csv(f"{DATADIR}MCR010_LAR_M_{date_today.strftime(format='%d-%b-%Y')}_{period_lar}.csv", index=False)
mcr010_l_df = wait_retry(get_mcr010(mcr_l_serie))
if not CRONTAB:
    mcr010_l_df.to_csv(f"{DATADIR}MCR010_LAR_L_{date_today.strftime(format='%d-%b-%Y')}_{period_lar}.csv", index=False)

# %% [markdown]
# #### MCR012
# 
# For **MCR012**, 
# 
# - *DPR* verison contains only 1 row of current period.
# - *LAR* version contains $12\times3$ rows ahead curren period.
# 

# %%
''' Get MCR012 '''
print(" MCR012", end="")
# DPR
mcr012_df = wait_retry(get_mcr012(mcr_serie))
if not CRONTAB: 
    mcr012_df.to_csv(f"{DATADIR}MCR012_DPR_{date_today.strftime(format='%d-%b-%Y')}_{period_now}.csv", index=False)
# LAR
mcr012_h_df = wait_retry(get_mcr012(mcr_h_serie))
if not CRONTAB: 
    mcr012_h_df.to_csv(f"{DATADIR}MCR012_LAR_H_{date_today.strftime(format='%d-%b-%Y')}_{period_lar}.csv", index=False)
mcr012_m_df = wait_retry(get_mcr012(mcr_m_serie))
if not CRONTAB: 
    mcr012_m_df.to_csv(f"{DATADIR}MCR012_LAR_M_{date_today.strftime(format='%d-%b-%Y')}_{period_lar}.csv", index=False)
mcr012_l_df = wait_retry(get_mcr012(mcr_l_serie))
if not CRONTAB: 
    mcr012_l_df.to_csv(f"{DATADIR}MCR012_LAR_L_{date_today.strftime(format='%d-%b-%Y')}_{period_lar}.csv", index=False)

# %% [markdown]
# ## Join Data

# %% [markdown]
# ### Methods

# %% [markdown]
# #### DPR

# %%
def mcr010_for_dpr(mcr010_df):   
    mcr010_df = mcr010_df.copy() 
    mcr010_df = mcr010_df[['ForecastDate', 'ForecastPeriod', 'CUSEP', 'TransmissionLoss', 'EnergyShortfall', 'RLQ']].copy()
    mcr010_df['ForecastPeriod'] = mcr010_df['ForecastPeriod'].astype(int)
    
    return mcr010_df

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

    dpr_df.fillna(0, inplace=True)

    dpr_df = dpr_df[['Date', 'Period',
                    'Demand', 'TCL', 'USEP', 'CUSEP',  'LCP', 'TransmissionLoss', 'EnergyShortfall',
                    'RLQ', 'Regulation', 'RegulationRequirement', 'RegulationShortfall',
                    'PrimaryReserve', 'PrimaryReserveRequirement', 'PrimaryReserveShortfall',
                    'ContingencyReserve', 'ContingencyReserveRequirement', 
                    'ContingencyReserveShortfall', 'EHEUR', 'Solar'
                    ]]
    
    print(".", end="")
    return dpr_df

# %% [markdown]
# #### LAR

# %%
def mcr010_for_lar(date_today, period_now, mcr010_df, load_scenario):
    mcr010_df = mcr010_df.copy() 
    mcr010_df = mcr010_df[['ForecastDate', 'ForecastPeriod', 'TotalLoad', 'TCL', 'USEP', 'CUSEP', 'LCP', 'TransmissionLoss', 'EnergyShortfall', 'RLQ', 'EHEUR', 'Solar']].copy()

    # mcr010_df['ForecastPeriod'] = mcr010_df['ForecastPeriod'].astype('int64')

    # Add current date and period back to dataframe.
    mcr010_df['Period'] = period_now
    mcr010_df['Date'] = date_today

    # Add load scenario info
    mcr010_df['LoadScenario'] = load_scenario

    mcr010_df.rename(
        {
            'TotalLoad': 'Demand'
        },
        axis=1,
        inplace=True
    )

    return mcr010_df

# %%
def mcr012_for_lar(date_today, period_now, mcr012_df):
    mcr012_df = mcr012_df.copy()
    
    # Flatten MCR012
    mcr012_data = {
        'ForecastDate': [],
        'ForecastPeriod': [],
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
            mcr012_data['ForecastDate'].append(row["ForecastDate"])
            mcr012_data['ForecastPeriod'].append(row["ForecastPeriod"])
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
    mcr012_df['Date'] = date_today
    mcr012_df['Period'] = period_now

    return mcr012_df

# %%
def get_lar(corp_peri_df: pd.DataFrame, mcr010_df, mcr012_df):
    
    mcr_df = pd.merge(mcr010_df, mcr012_df, how='left',
                      on=['ForecastDate', 'ForecastPeriod','Date','Period'])
    corp_peri_df.rename(
        {
            "Date": "ForecastDate",
            "Period": "ForecastPeriod"
        },
        axis=1,
        inplace=True
    )

    corp_peri_df = corp_peri_df[[
        "ForecastDate", "ForecastPeriod", 
        'Regulation', 'PrimaryReserve', 'ContingencyReserve'
        ]]

    # return corp_peri_df

    lar_df = pd.merge(corp_peri_df, mcr_df, how='left',
                      on=['ForecastDate', 'ForecastPeriod'])
    # return lar_df

    float64_cols = ['Demand', 'TCL', 'USEP', 'CUSEP', 'LCP', 'TransmissionLoss',
                    'EnergyShortfall', 'RLQ', 'EHEUR', 'RegulationRequirement', 'Regulation', 
                    'RegulationShortfall', 'PrimaryReserveRequirement', 'PrimaryReserve', 
                    'PrimaryReserveShortfall', 'ContingencyReserveRequirement', 
                    'ContingencyReserve', 'ContingencyReserveShortfall', 'Solar']

    lar_df[float64_cols] = lar_df[float64_cols].astype('float64')

    # lar_df['Date'] = lar_df['Date'].dt.date
    # lar_df['ForecastDate'] = lar_df['ForecastDate'].dt.date

    lar_df.fillna(0, inplace=True)

    lar_df = lar_df[['Date', 'Period', 'LoadScenario', 'ForecastDate', 'ForecastPeriod',
                    'Demand', 'TCL', 'USEP', 'CUSEP', 'LCP', 'TransmissionLoss',
                     'EnergyShortfall', 'RLQ', 'EHEUR', 'RegulationRequirement', 'Regulation', 
                     'RegulationShortfall', 'PrimaryReserveRequirement', 'PrimaryReserve', 
                     'PrimaryReserveShortfall', 'ContingencyReserveRequirement', 
                     'ContingencyReserve', 'ContingencyReserveShortfall', 'Solar'
                    ]]

    print(".", end="")
    return lar_df

# %% [markdown]
# ### Main Process

# %% [markdown]
# #### Debugging Startpoint
# 
# If data has been stored into `.csv` file, can load from here to debug following code.

# %%
DEBUG = False

# %%
def date_as_date(df: pd.DataFrame):
    df = df.copy()
    for col in df.columns:
        if "Date" in col:
            df[col] = pd.to_datetime(df[col])
            df[col] = df[col].dt.date
    return df

# %%
if DEBUG:
    print("Using debugging dataset")
    corp_today = pd.read_csv(join(DATADIR, 'Corp_2024-04-16.csv'))
    corp_today = date_as_date(corp_today)
    corp_tomorrow = pd.read_csv(join(DATADIR, 'Corp_2024-04-17.csv'))
    corp_tomorrow = date_as_date(corp_tomorrow)

    mcr_df = pd.read_csv(join(DATADIR, 'MCR001_DPR_16-Apr-2024_46.csv'))
    mcr_df = date_as_date(mcr_df)
    mcr_serie = mcr_df[
        (mcr_df['FirstDate'] == date_today) & 
        (mcr_df['FirstPeriod'] == period_now)].copy()

    mcr_h_df = pd.read_csv(join(DATADIR, 'MCR001_LAR_H_16-Apr-2024_47.csv'))
    mcr_h_df = date_as_date(mcr_h_df)
    mcr_h_serie = mcr_h_df[
        (mcr_h_df['FirstDate'] == date_lar) & 
        (mcr_h_df['FirstPeriod'] == period_lar)].copy()

    mcr_m_df = pd.read_csv(join(DATADIR, 'MCR001_LAR_M_16-Apr-2024_47.csv'))
    mcr_m_df = date_as_date(mcr_m_df)
    mcr_m_serie = mcr_m_df[
        (mcr_m_df['FirstDate'] == date_lar) & 
        (mcr_m_df['FirstPeriod'] == period_lar)].copy()

    mcr_l_df = pd.read_csv(join(DATADIR, 'MCR001_LAR_L_16-Apr-2024_47.csv'))
    mcr_l_df = date_as_date(mcr_l_df)
    mcr_l_serie = mcr_l_df[
        (mcr_l_df['FirstDate'] == date_lar) & 
        (mcr_l_df['FirstPeriod'] == period_lar)].copy()

    mcr010_df = pd.read_csv(join(DATADIR, 'MCR010_DPR_16-Apr-2024_46.csv'))
    mcr010_df = date_as_date(mcr010_df)
    mcr010_h_df = pd.read_csv(join(DATADIR, 'MCR010_LAR_H_16-Apr-2024_47.csv'))
    mcr010_h_df = date_as_date(mcr010_h_df)
    mcr010_m_df = pd.read_csv(join(DATADIR, 'MCR010_LAR_M_16-Apr-2024_47.csv'))
    mcr010_m_df = date_as_date(mcr010_m_df)
    mcr010_l_df = pd.read_csv(join(DATADIR, 'MCR010_LAR_L_16-Apr-2024_47.csv'))
    mcr010_l_df = date_as_date(mcr010_l_df)

    mcr012_df = pd.read_csv(join(DATADIR, 'MCR012_DPR_16-Apr-2024_46.csv'))
    mcr012_df = date_as_date(mcr012_df)
    mcr012_h_df = pd.read_csv(join(DATADIR, 'MCR012_LAR_H_16-Apr-2024_47.csv'))
    mcr012_h_df = date_as_date(mcr012_h_df)
    mcr012_m_df = pd.read_csv(join(DATADIR, 'MCR012_LAR_M_16-Apr-2024_47.csv'))
    mcr012_m_df = date_as_date(mcr012_m_df)
    mcr012_l_df = pd.read_csv(join(DATADIR, 'MCR012_LAR_L_16-Apr-2024_47.csv'))
    mcr012_l_df = date_as_date(mcr012_l_df)

# %% [markdown]
# #### DPR

# %%
''' DPR '''
print(" DPR", end='')
# DPR required data
corp_peri_df = corp_today[
    (corp_today['Date'] == date_today) & 
    (corp_today['Period'] == period_now)].copy()

mcr010_dpr_df = mcr010_for_dpr(mcr010_df)
mcr012_dpr_df = mcr012_for_dpr(mcr012_df)

# Construct DPR
dpr_df = get_dpr(corp_peri_df, mcr010_dpr_df, mcr012_dpr_df)

# %% [markdown]
# #### LAR
# 
# To construct **LAR** dataframe, for every scenario, there should be 12 rows data.

# %%
print(" LAR", end='')
# LAR required data
corp_df = corp_today[
    (corp_today['Date'] == date_lar) &
    (corp_today['Period'] >= period_lar) &
    (corp_today['Period'] < period_lar + 12)
    ].copy()

# %%
if need_tomorrow_data:
    # Example 1: now is period 37, then LAR data range involves
    #   - Today: period 38 - 48, 11 periods in total.
    #   - Tomorrow: period 1, 1 period in total.
    
    # Example 2, now is period 48, then LAR data range involves
    #   - Today: No data.
    #   - Tomorrow: period 1 - 12, 12 period in total.
    
    count_period_today = 48 - period_now
    period_lar_end = 12 - count_period_today
    # print(count_period_today, count_period_tomorrow, period_lar_end)
    
    corp_df_tomorrow = corp_tomorrow[
        (corp_tomorrow['Date'] == date_tomorrow) & 
        (corp_tomorrow['Period'] <= period_lar_end)].copy()

    corp_df = pd.concat([corp_df, corp_df_tomorrow])

# %%
''' LAR H '''
# Construct LAR
mcr010_lar_df = mcr010_for_lar(date_today, period_now, mcr010_h_df, 'H')
mcr012_lar_df = mcr012_for_lar(date_today, period_now, mcr012_h_df)
lar_h_df = get_lar(corp_df, mcr010_lar_df, mcr012_lar_df)


''' LAR M '''
# Construct LAR
mcr010_lar_df = mcr010_for_lar(date_today, period_now, mcr010_m_df, 'M')
mcr012_lar_df = mcr012_for_lar(date_today, period_now, mcr012_m_df)
lar_m_df = get_lar(corp_df, mcr010_lar_df, mcr012_lar_df)


''' LAR L '''
# Construct LAR
mcr010_lar_df = mcr010_for_lar(date_today, period_now, mcr010_l_df, 'L')
mcr012_lar_df = mcr012_for_lar(date_today, period_now, mcr012_l_df)
lar_l_df = get_lar(corp_df, mcr010_lar_df, mcr012_lar_df)

# %% [markdown]
# ## Save to DB

# %% [markdown]
# ### Methods

# %% [markdown]
# #### DB Connection

# %%
# Load the environment variables from the .env file
env_file = join(ROOT, '.env')
load_dotenv(env_file)

# Get the values of host, user, pswd, db, and schema from the environment variables
host = os.getenv('host')
user = os.getenv('user')
pswd = os.getenv('pswd')
db = os.getenv('db')
# schema = os.getenv('schema')
schema = 'public'

# Use the values as needed
engine = create_engine(f"postgresql://{user}:{pswd}@{host}/{db}?options=-csearch_path%3D{schema}", echo=False)
conn = engine.connect()

# %% [markdown]
# #### DPR to DB

# %%
def db_dpr(dpr_df):
    
    dpr_se = dpr_df.iloc[0]
    
    # check existing row
    row_exists_query = f"""
        SELECT 1 FROM public."RealTimeDPR"
            WHERE "Date" = '{dpr_se['Date'].strftime(format='%Y-%m-%d')}' AND "Period" = '{dpr_se['Period']}';
        """
    row_exists = conn.execute(text(row_exists_query)).scalar()

    if row_exists:

        set_pairs = []
        d = dpr_se.to_dict()
        for k,v in d.items():
            
            if 'Date' in k:
                set_pairs.append(f""""{k}"='{v.strftime(format="%Y-%m-%d")}'""")
                continue

            set_pairs.append(f""""{k}"={float(v)}""")
        
        update_query = f'''
            UPDATE public."RealTimeDPR" 
                SET {", ".join(set_pairs)}
                WHERE 
                    "Date" = '{dpr_se['Date'].strftime(format='%Y-%m-%d')}' AND 
                    "Period" = {dpr_se['Period']}
            ;
            '''
        # print(update_query)
        conn.execute(text(update_query))
        conn.commit()
        print("*", end="")
        return 0
    
    else:
        cols = []
        values = []
        d = dpr_se.to_dict()
        for k,v in d.items():
            cols.append(f'''"{k}"''')
            
            if 'Date' in k:
                values.append(f"""'{v.strftime(format="%Y-%m-%d")}'""")
                continue

            values.append(f"{float(v)}")
        
        insert_query = f'''INSERT INTO public."RealTimeDPR" ({", ".join(cols)}) \n VALUEs ({", ".join(values)});'''
        # print(insert_query)
        conn.execute(text(insert_query))
        conn.commit()
        print(".", end="")
        return 1

# %% [markdown]
# #### LAR to DB

# %%
def db_lar(lar_df: pd.DataFrame):
    new = 0
    exist = 0

    for row in range(lar_df.shape[0]):
        lar_se = lar_df.iloc[row]

        # check existing row
        row_exists_query = f"""
            SELECT 1 FROM public."RealTimeLAR"
                WHERE 
                    "Date"='{lar_se['Date'].strftime(format="%Y-%m-%d")}' AND 
                    "Period"={lar_se['Period']} AND 
                    "LoadScenario"='{lar_se['LoadScenario']}' AND
                    "ForecastDate"='{lar_se['ForecastDate'].strftime(format="%Y-%m-%d")}' AND 
                    "ForecastPeriod"={lar_se['ForecastPeriod']}  
            ;
            """
        # print(row_exists_query)
        row_exists = conn.execute(text(row_exists_query)).scalar()
        # print(row_exists)
        
        # continue

        if row_exists:

            set_pairs = []
            d = lar_se.to_dict()
            for k,v in d.items():
                
                if 'Date' in k:
                    set_pairs.append(f""""{k}"='{v.strftime(format="%Y-%m-%d")}'""")
                    continue

                if 'Scenario' in k:
                    set_pairs.append(f""""{k}"='{v}'""")
                    continue

                set_pairs.append(f""""{k}"={float(v)}""")
            
            update_query = f'''
                UPDATE public."RealTimeLAR" 
                    SET {", ".join(set_pairs)}
                    WHERE 
                        "Date"='{lar_se['Date'].strftime(format="%Y-%m-%d")}' AND 
                        "Period"={lar_se['Period']} AND 
                        "LoadScenario"='{lar_se['LoadScenario']}' AND
                        "ForecastDate"='{lar_se['ForecastDate'].strftime(format="%Y-%m-%d")}' AND 
                        "ForecastPeriod"={lar_se['ForecastPeriod']} 
                ;
                '''
            # print(update_query)

            conn.execute(text(update_query))
            conn.commit()
            print("*", end="")
            
            exist += 1
        else:
            cols = []
            values = []
            d = lar_se.to_dict()
            for k,v in d.items():
                cols.append(f'''"{k}"''')
                
                if 'Date' in k:
                    values.append(f"""'{v.strftime(format="%Y-%m-%d")}'""")
                    continue
                
                if 'Scenario' in k:
                    values.append(f"""'{v}'""")
                    continue

                values.append(f"{float(v)}")
            
            insert_query = f'''INSERT INTO public."RealTimeLAR" ({", ".join(cols)}) \n VALUEs ({", ".join(values)});'''
            # print(insert_query)

            conn.execute(text(insert_query))
            conn.commit()
            print(".", end="")

            new += 1

    return new, exist

# %% [markdown]
# ### Main Process

# %%
# This is used to create the "RealTimeDPR" table.
# dpr_df.to_sql("RealTimeDPR", conn, index=False, if_exists='replace')

# %%
exist = 0
new = 0

''' DPR '''
print(" DPR", end='')
add_new = db_dpr(dpr_df)

if add_new: 
    new += 1
else: exist += 1

conn.commit()

# %%
# This is used to create the "RealTimeLAR" table.
# lar_h_df.to_sql("RealTimeLAR", conn, index=False, if_exists='replace')

# %%
print(" LAR", end='')

for lar in [lar_h_df, lar_m_df, lar_l_df]:
    add_new, add_exist = db_lar(lar)

    new +=add_new
    exist += add_exist


# %%
conn.commit()
conn.close()

# %%
print(" All done!", end='')
print(f" Updated {exist} rows. Added {new} rows.")

# %%



