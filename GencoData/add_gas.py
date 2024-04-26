import PyPDF2
import os
import re
from datetime import datetime, timedelta
import pandas as pd
import psycopg2

# Get the list of files in the current directory
folder_path = 'DR_DemandForecast/GencoData'
files = os.listdir(folder_path)
# Filter the filenames to find the one that starts with "PGCS_2024"
matching_files = [file for file in files if file.startswith("PGCS_2024")]

if matching_files:
    target_file = matching_files[0]
    file_path = folder_path + '/' + target_file
    with open(file_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)

        text = ''
        # Loop through each page in the PDF
        for page_num in range(len(reader.pages)):
            # Extract text from the current page
            page = reader.pages[page_num]
            text += page.extract_text()
        
        # Define the regex pattern to match date, time, date, time, and percentage
        pattern = r'(\d{2}/\d{2}/\d{2}).*?(\d{2}/\d{2}/\d{2}).*?(0%|\d+\s*-\s*\d+\s*%)'

        # Find all matches in the text
        matches = re.findall(pattern, text)

        # Function to expand date ranges and assign percentages
        def expand_date_ranges(data):
            expanded_data = []
            for start_date, end_date, percentage in data:
                # Parse start and end dates
                start_date = datetime.strptime(start_date, '%d/%m/%y')
                end_date = datetime.strptime(end_date, '%d/%m/%y')

                # Generate dates and assign percentages
                date = start_date
                while date <= end_date:
                    expanded_data.append((date.strftime('%d/%m/%y'), percentage))
                    date += timedelta(days=1)

            return expanded_data

        # Expand date ranges and assign percentages
        expanded_data = expand_date_ranges(matches)

        # Create DataFrame
        df = pd.DataFrame(expanded_data, columns=['Date', 'Percentage'])
        df['Percentage'] = df['Percentage'].str.replace(r'\s|%','', regex=True)

        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%y').dt.strftime('%Y-%m-%d')


        # Add to db
        ENDPOINT = "postgres-1.cvh49u2v99nl.ap-southeast-1.rds.amazonaws.com"
        PORT = "5432"
        USER = "sdcmktops"
        REGION = "ap-southeast-1c"
        DBNAME = "postgres"
        password = "SDCsdc1234"

        print('Connecting to PGCS DB')

        conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=password)
        cur = conn.cursor()

        # Define SQL query
        query = """
            INSERT INTO public."PGCS" ("Date", "Curtailment")
            VALUES (%s, %s)
            ON CONFLICT ("Date") DO UPDATE SET "Curtailment" = EXCLUDED."Curtailment";
            """

        for row in df.itertuples(index=False):
            cur.execute(query, row)
        
        print('Successful Commit')
        conn.commit()
        conn.close()


else:
    print("No PGCS file found")