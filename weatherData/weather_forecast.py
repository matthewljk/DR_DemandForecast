import requests

# url = "https://api.openweathermap.org/data/2.5/forecast?lat=1.3562&lon=103.849&appid=52eefa599155552610c8a6abb7659b98"

url = 'https://api.openweathermap.org/data/2.5/weather?lat=1.3562&lon=103.849&exclude=[minutely,daily,alerts,current]&appid=52eefa599155552610c8a6abb7659b98'

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)


if response.status_code == 200:
    data = response.json()
    print(data)

else:
    print("Failed to retrieve data")