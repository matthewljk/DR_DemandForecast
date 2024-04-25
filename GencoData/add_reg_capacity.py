import pandas as pd
import psycopg2

# Replace with latest csv file
file_path = 'DR_DemandForecast/GencoData/RC_25Apr2024.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(file_path)
df = df.drop(columns=['INFORMATION TYPE','DATE',
                      'COMMISSIONING GENERATION FACILITY','MAX SECONDARY RESERVE CAPACITY (MW)'])

# Rename col
column_mapping = {
    'MARKET PARTICIPANT': 'MP',
    'FACILITY NAME': 'Facility',
    'FACILITY REGISTRATION STATUS': 'Status',
    'FACILITY GENERATION TYPE': 'Type',
    'EMBEDDED STATUS': 'EG',
    'MAX GENERATION CAPACITY (MW)': 'Max Gen',
    'MIN STABLE LOAD (MW)': 'MSL',
    'MAX LOAD CURTAILMENT CAPACITY (MW)': 'Max Load',
    'MAX REGULATION CAPACITY (MW)': 'Max Reg',
    'MAX PRIMARY RESERVE CAPACITY (MW)': 'Max Pri',
    'MAX CONTINGENCY RESERVE CAPACITY (MW)': 'Max Contingency',
    'FREQUENCY RESPONSIVE STATUS': 'FR Status',
    'LAST CHANGE DATE': 'Last Change'
}
df = df.rename(columns=column_mapping)

# Error handling
df = df.replace('-', 0)

df['Max Gen'] = df['Max Gen'].astype(float)
df['MSL'] = df['MSL'].astype(float)
df['Max Load'] = df['Max Load'].astype(float)
df['Max Reg'] = df['Max Reg'].astype(float)
df['Max Pri'] = df['Max Pri'].astype(float)
df['Max Contingency'] = df['Max Contingency'].astype(float)
df['Last Change'] = pd.to_datetime(df['Last Change'], format='%d-%b-%Y', errors='coerce')

print(df)

# Add to DB
ENDPOINT = "postgres-1.cvh49u2v99nl.ap-southeast-1.rds.amazonaws.com"
PORT = "5432"
USER = "sdcmktops"
REGION = "ap-southeast-1c"
DBNAME = "postgres"
password = "SDCsdc1234"

print('Connecting to Reg_Capacity DB')

try:
    conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=password)
    cur = conn.cursor()

    for index,row in df.iterrows():
        query = f'''
            INSERT INTO public."Reg_Capacity"
            VALUES ('{row['MP']}','{row['Facility']}','{str(row['Status'])}','{row['Type']}','{row['EG']}',
            '{row['Max Gen']}','{row['MSL']}',{row['Max Load']},{row['Max Reg']}
            ,{row['Max Pri']},{row['Max Contingency']},'{row['FR Status']}','{row['Last Change']}')
            ON CONFLICT ("MP","Facility")
            DO
            UPDATE SET "Status" = '{str(row['Status'])}', "Type" = '{str(row['Type'])}', "EG" = '{row['EG']}'
            , "Max Gen" = {row['Max Gen']}, "MSL" = {row['MSL']}, "Max Load" = {row['Max Load']}
            , "Max Reg" = {row['Max Reg']}, "Max Contingency" = {row['Max Contingency']}
            , "FR Status" = '{row['FR Status']}', "Last Change" = '{row['Last Change']}';
        '''
        cur.execute(query=query)
        conn.commit()
    
    print("Successful connection")

except Exception as e:
    print("Error: "+ str(e))