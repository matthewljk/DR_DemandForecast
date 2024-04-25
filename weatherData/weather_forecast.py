import requests
import urllib3
from datetime import datetime
import psycopg2
import pandas as pd

import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

lat = 1.3562
lon = 103.849
api_key = '52eefa599155552610c8a6abb7659b98'
def get_current_weather(lat:float, lon:float, api_key):
    url = f"https://api.openweathermap.org/data/2.5/forecast?lat={str(lat)}&lon={str(lon)}&appid={api_key}&units=metric"
    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return None

def process_forecast_data(data):

    latitude = data['city']['coord']['lat']
    longtitude = data['city']['coord']['lon']
    data_list = data['list']

    forecast_df = pd.DataFrame({
        'Date': [],
        'Period': [],
        'Timestamp': [],
        'Lat': [],
        'Lon': [],
        'Main': [],
        'Des': [],
        'Temp': [],
        'Feels_like': [],
        'Temp_min': [],
        'Temp_max': [],
        'Pressure': [],
        'Humidity': [],
        'Wind_speed': [],
        'Wind_deg': [],
        'Pop':[],
        'Rain': [],
        'Clouds': [],
    })

    for interval in range(len(data_list)):
        dt = data_list[interval]['dt']
        dt_object = datetime.fromtimestamp(dt)
        date = dt_object.strftime('%Y-%m-%d')
        timestamp = dt_object.strftime('%H:%M:%S')
        hour = dt_object.strftime('%H')
        min = dt_object.strftime('%M')
        period = (int(hour) * 2) + (int(min)//30) + 1

        weather_main = data_list[interval]['weather'][0]['main']
        weather_des = data_list[interval]['weather'][0]['description']
        temp = data_list[interval]['main']['temp']
        feels_like = data_list[interval]['main']['feels_like']
        temp_min = data_list[interval]['main']['temp_min']
        temp_max = data_list[interval]['main']['temp_max']
        pressure = data_list[interval]['main']['pressure']
        humidity = data_list[interval]['main']['humidity']
        wind_speed = data_list[interval]['wind']['speed']
        wind_deg = data_list[interval]['wind']['deg']
        if data_list[interval].get('rain'):
            rain = data_list[interval]['rain']['3h']
        else:
            rain = 0
        pop = data_list[interval]['pop']
        clouds = data_list[interval]['clouds']['all']

        new_row = {
            'Date': [date],
            'Period': [period],
            'Timestamp': [timestamp],
            'Lat': [latitude],
            'Lon': [longtitude],
            'Main': [weather_main],
            'Des': [weather_des],
            'Temp': [temp],
            'Feels_like': [feels_like],
            'Temp_min': [temp_min],
            'Temp_max': [temp_max],
            'Pressure': [pressure],
            'Humidity': [humidity],
            'Wind_speed': [wind_speed],
            'Wind_deg': [wind_deg],
            'Pop': [pop],
            'Rain': [rain],
            'Clouds': [clouds],
        }
        forecast_df = pd.concat([forecast_df,pd.DataFrame(new_row)],ignore_index=True)
        
    return forecast_df

def add_to_db(df):
    ENDPOINT = "postgres-1.cvh49u2v99nl.ap-southeast-1.rds.amazonaws.com"
    PORT = "5432"
    USER = "sdcmktops"
    REGION = "ap-southeast-1c"
    DBNAME = "postgres"
    password = "SDCsdc1234"

    print('Connecting to DB')

    try:
        conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=password)
        cur = conn.cursor()

        for index,row in df.iterrows():
            query = f'''
                INSERT INTO public."Weather_Forecast"
                VALUES ('{row['Date']}',{row['Period']},'{str(row['Timestamp'])}',{row['Lat']},{row['Lon']},'{row['Main']}','{row['Des']}',{row['Temp']},{row['Feels_like']}
                ,{row['Temp_min']},{row['Temp_max']},{row['Pressure']},{row['Humidity']},{row['Wind_speed']},{row['Wind_deg']},{row['Pop']},{row['Rain']},{row['Clouds']})
                ON CONFLICT ("Date","Period","Timestamp","Lat","Lon")
                DO
                UPDATE SET "Main" = '{str(row['Main'])}', "Des" = '{str(row['Des'])}', "Temp" = {row['Temp']}, "Feels_like" = {row['Feels_like']}
                , "Temp_max" = {row['Temp_max']}, "Temp_min" = {row['Temp_min']}, "Pressure" = {row['Pressure']}, "Humidity" = {row['Humidity']}, "Wind_speed" = {row['Wind_speed']}
                , "Wind_deg" = {row['Wind_deg']}, "Pop" = {row['Pop']}, "Rain" = {row['Rain']}, "Clouds" = {row['Clouds']};
            '''
            cur.execute(query=query)
            conn.commit()
        
        print("Successful connection")

        return True

    except Exception as e:
        print("Error: "+ str(e))
        return None

data = get_current_weather(lat=lat,lon=lon,api_key=api_key)
forecast_df = process_forecast_data(data)
add_to_db(forecast_df)