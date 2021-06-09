from pandas.core.indexes.datetimes import date_range
from pandas.tseries.offsets import DateOffset
import requests
import pandas as pd
from datetime import *
from numpy import logical_and

api_key = ''
daily_api_key = ''

def clean_weather_data(api_result, frequency, location):
    #transform data
    if frequency == 'daily':
        clean_weather = pd.json_normalize(data=api_result['daily'], 
            record_path='weather',
            meta=['dt','temp','feels_like'])
        clean_weather = clean_weather.drop(['main', 'icon'], 1)
        clean_weather['dt'] = clean_weather['dt'].apply \
            (lambda x:datetime.fromtimestamp(x))
        clean_weather['temp'] = clean_weather['temp'].apply(lambda x: x['day'])
        clean_weather['feels_like'] = clean_weather['feels_like'].apply(lambda x: x['day'])
        
    elif frequency == 'hourly':
        clean_weather = pd.json_normalize(data=api_result['hourly'], 
            record_path='weather',
            meta=['dt','temp','feels_like'])
        clean_weather = clean_weather.drop(['main', 'icon'], 1)
        clean_weather['dt'] = clean_weather['dt'].apply \
            (lambda x:datetime.fromtimestamp(x))
       
    clean_weather['location'] = location
    # filter the dates * drop duplicates
    start_date = datetime(2021, 6, 3)
    end_date = datetime(2021, 6, 10)
    clean_weather = clean_weather[logical_and(clean_weather['dt'] >= start_date, \
        clean_weather['dt'] <= end_date)]
    clean_weather['dt'] = clean_weather['dt'].apply(lambda x: x.strftime('%m-%d-%y'))
    clean_weather = clean_weather.drop_duplicates(subset='dt')
    return clean_weather

def get_next_six_days_data(daily_api_key):
    url = url = 'https://api.openweathermap.org/data/2.5/onecall'
    #For Detroit
    paramts ={
        'lat':42.331429,
        'lon':-83.045753,
        'appid':daily_api_key,
        'units':'imperial',
        'exclude':['minutely','hourly','alerts']
        }
    #For LA
    paramts_two ={
        'lat':37.775171,
        'lon':-122.41927,
        'appid':daily_api_key,
        'units':'imperial',
        'exclude':['minutely','hourly','alerts'] 
    }
    # Getting data for LA and Detriot 
    detroit_weather_data = requests.get(url=url, params=paramts)
    losangeles_weather_data = requests.get(url=url, params=paramts_two)
    # result json
    detroit_result_json = detroit_weather_data.json()
    losangeles_result_json = losangeles_weather_data.json()

    # call clean weather data function
    detroit = clean_weather_data(detroit_result_json, frequency='daily', location='Detroit')
    la = clean_weather_data(losangeles_result_json, frequency='daily', location='Los Angeles')
    clean_weather= pd.concat([detroit, la])
    #write to csv file
    clean_weather.to_csv('curated_first.csv', index=False)

def get_previous_weather(api_key):
    #get previous days data
    url = 'https://api.openweathermap.org/data/2.5/onecall/timemachine'   
    clean_weather = pd.DataFrame()
    for i in range(3,8):
        # For Detroit
        previous_day = round(datetime(2021, 6, i).timestamp())
        paramts = {
                'lat':42.331429,
                'lon':-83.045753,
                'dt':previous_day,
                'appid':api_key,
                'units':'imperial'                          
                }
        #for LA
        paramts_two ={
            'lat':37.775171,
            'lon':-122.41927,
            'dt':previous_day,
            'appid':api_key,
            'units':'imperial'
            }
        # send requests
        weather_result_detroit = requests.get(url=url, params=paramts)
        weather_result_losangeles = requests.get(url=url, params=paramts_two)
        #result json
        detroit_result = weather_result_detroit.json()
        la_result = weather_result_losangeles.json()
        #function call
        clean_detroit_data = clean_weather_data(detroit_result, 'hourly', 'Detroit')
        clean_la_data = clean_weather_data(la_result, 'hourly', 'Los Angeles')    
        clean_weather = pd.concat([clean_weather, clean_detroit_data, clean_la_data])

    clean_weather.sort_values('dt', inplace=True)    
    clean_weather.to_csv('curated_second.csv')
  
def merge_files():
    #Merge files
    first_file = pd.read_csv('curated_first.csv')
    second_file = pd.read_csv('curated_second.csv')
    frames = [first_file, second_file]
    merged = pd.DataFrame()
    merged = pd.concat(frames, sort=True)
    merged.drop(merged.filter(regex='Unnamed'),axis=1,inplace=True)
    merged = merged[['id', 'description', 'dt', 'temp', 'feels_like', 'location']]
    merged.to_csv('output.csv', index=0)  
def main():
    get_previous_weather(api_key)
    get_next_six_days_data(daily_api_key)
    merge_files()
if __name__=='__main__':
    main()

