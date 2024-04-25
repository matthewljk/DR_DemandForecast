import requests
import urllib3
from datetime import datetime
import psycopg2
import pandas as pd

import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# # url = "https://api.openweathermap.org/data/2.5/forecast?lat=1.3562&lon=103.849&appid=52eefa599155552610c8a6abb7659b98"

# url = 'https://api.openweathermap.org/data/2.5/weather?lat=1.3562&lon=103.849&exclude=[daily,alerts,current]&appid=52eefa599155552610c8a6abb7659b98'

# payload = {}
# headers = {}

# response = requests.request("GET", url, headers=headers, data=payload)

# if response.status_code == 200:
#     data = response.json()
#     print(data)

# else:
#     print("Failed to retrieve data")

lat = 1.3562
lon = 103.849
api_key = '52eefa599155552610c8a6abb7659b98'
def get_current_weather(lat:float, lon:float, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={str(lat)}&lon={str(lon)}&exclude=[daily,alerts,current,minutely]&appid={api_key}&units=metric"
    payload = {}
    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return None
    
def process_current_data(data):
    latitude = data['coord']['lat']
    longtitude = data['coord']['lon']
    weather_main = data['weather'][0]['main']
    weather_des = data['weather'][0]['description']
    temp = data['main']['temp']
    feels_like = data['main']['feels_like']
    temp_min = data['main']['temp_min']
    temp_max = data['main']['temp_max']
    pressure = data['main']['pressure']
    humidity = data['main']['humidity']
    wind_speed = data['wind']['speed']
    wind_deg = data['wind']['deg']
    if data.get('rain'):
        rain = data['rain']['1h']
    else:
        rain = 0
    clouds = data['clouds']['all']
    dt = data['dt']
    dt_object = datetime.fromtimestamp(dt)
    date = dt_object.strftime('%Y-%m-%d')
    timestamp = dt_object.strftime('%H:%M:%S')
    hour = dt_object.strftime('%H')
    min = dt_object.strftime('%M')
    period = (int(hour) * 2) + (int(min)//30) + 1

    # Create DataFrame
    weather_df = pd.DataFrame({
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
        'Rain': [rain],
        'Clouds': [clouds],
    })
    return weather_df

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
                INSERT INTO public."Weather_Data"
                VALUES ('{row['Date']}',{row['Period']},'{str(row['Timestamp'])}',{row['Lat']},{row['Lon']},'{row['Main']}','{row['Des']}',{row['Temp']},{row['Feels_like']}
                ,{row['Temp_min']},{row['Temp_max']},{row['Pressure']},{row['Humidity']},{row['Wind_speed']},{row['Wind_deg']},{row['Rain']},{row['Clouds']})
                ON CONFLICT ("Date","Period","Timestamp","Lat","Lon")
                DO
                UPDATE SET "Main" = '{str(row['Main'])}', "Des" = '{str(row['Des'])}', "Temp" = {row['Temp']}, "Feels_like" = {row['Feels_like']}
                , "Temp_max" = {row['Temp_max']}, "Temp_min" = {row['Temp_min']}, "Pressure" = {row['Pressure']}, "Humidity" = {row['Humidity']}, "Wind_speed" = {row['Wind_speed']}
                , "Wind_deg" = {row['Wind_deg']}, "Rain" = {row['Rain']}, "Clouds" = {row['Clouds']};
            '''        
            cur.execute(query=query)
            conn.commit()
        
        print("Successful connection")

        return True

    except Exception as e:
        print("Error: "+ str(e))
        return None

current_data = get_current_weather(lat=lat, lon=lon, api_key=api_key)
weather_df = process_current_data(current_data)
add_to_db(weather_df)