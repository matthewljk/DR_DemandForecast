import PyPDF2
import re
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql

pdf_file_path = '/Users/matthew/Desktop/SDC_Demand_Forecast/DR_DemandForecast/GencoData/PGCS_2024__Updated_20240424_1907hrs_.pdf'

with open(pdf_file_path, 'rb') as file:
    # Create a PDF file reader object 
    pdf_reader = PyPDF2.PdfReader(file)
    text = ''

    for page_num in range(len(pdf_reader.pages)):
        # Get the text content of the current page
        page = pdf_reader.pages[page_num]
        text += page.extract_text()

pattern = r'(\d{2}/\d{2}/\d{2} \d{1,2}:\d{2}) (\d{2}/\d{2}/\d{2} \d{1,2}:\d{2}) (\d+%|\d+ - \d+ %)'

# Find all matches
matches = re.findall(pattern, text)

# Helper function to generate date range
def generate_date_range(start, end, percentage):
    start_date = datetime.strptime(start, '%d/%m/%y %H:%M')
    end_date = datetime.strptime(end, '%d/%m/%y %H:%M')
    delta = end_date - start_date
    dates = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    # Remove '%' and store as numbers
    clean_percentage = percentage.replace('%', '').strip()
    return [(date.strftime('%Y-%m-%d'), clean_percentage) for date in dates]

# Generate a list of dates with their corresponding percentages
date_percentage_list = []
for match in matches:
    date_percentage_list.extend(generate_date_range(match[0], match[1], match[2]))

# Create a DataFrame
df = pd.DataFrame(date_percentage_list, columns=['Date', 'Percentage'])

# Print the DataFrame
print(df)

# Add to DB
ENDPOINT = "postgres-1.cvh49u2v99nl.ap-southeast-1.rds.amazonaws.com"
PORT = "5432"
USER = "sdcmktops"
REGION = "ap-southeast-1c"
DBNAME = "postgres"
password = "SDCsdc1234"

# Connect to the PostgreSQL database
conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=password)
cursor = conn.cursor()

# Insert or update data
for index, row in df.iterrows():
    insert_query = sql.SQL("""
    INSERT INTO public."PGCS" ("Date", "Curtailment") 
    VALUES ({date}, {percentage})
    ON CONFLICT ("Date") DO UPDATE
    SET "Curtailment" = EXCLUDED."Curtailment";
    """).format(
        date=sql.Literal(row['Date']),
        percentage=sql.Literal(row['Percentage'])
    )
    cursor.execute(insert_query)

# Commit the transaction and close the connection
conn.commit()
cursor.close()
conn.close()