import os
import pandas as pd

import openpyxl

# Get the list of files in the current directory
folder_path = 'DR_DemandForecast/GencoData'
files = os.listdir(folder_path)
# Filter the filenames to find the one that starts with "agop_2024"
matching_files = [file for file in files if file.startswith("AGOP_2024")]

# Check if any matching file was found
if matching_files:
    # Assuming there is only one matching file, read it into a DataFrame
    target_file = matching_files[0]
    file_path = folder_path + '/' + target_file

    # # wb = openpyxl.load_workbook(filename=file_path, read_only=True, keep_links=False)
    # # Access the worksheet
    # ws = wb.active

    # # Iterate over rows and columns
    # for row in ws.iter_rows():
    #     for cell in row:
    #         print(cell.value)

    # # Close the workbook
    # wb.close()


    df = pd.read_excel(file_path)
    # # Display the DataFrame

    # df['Maintenance'] = df.iloc[:, 1:].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
    # df = df[['Generating Unit', 'Maintenance']]

    # print(df)
else:
    print("No file found that starts with 'AGOP_2024'")