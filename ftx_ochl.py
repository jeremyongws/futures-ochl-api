import json
import requests
import pdb
from datetime import datetime
import csv

BASE_URL = 'https://ftx.com/api'
RESOLUTION = 86400 #daily
LIMIT = 30
markets = ['BTC-1231','BTC_USDT']

def timestamp_to_date(timestamp):
    time_format = '%m-%d-%Y %H:%M:%S'
    #API returns ms, python uses s
    return datetime.utcfromtimestamp(timestamp/1000).strftime(time_format)

def getKlineData(pair,interval='1d'):
    symbol = pair
    req_url = 'https://fapi.binance.com/fapi/v1/klines?'
    req_url += 'symbol='+symbol+'&interval='+interval+'&limit=30'
    res = requests.get(req_url)
    data = res.json()
    formatted_data = []
    for day_data in data:
        day_data[0] = timestamp_to_date(day_data[0])
        day_data = day_data[:5]
        day_data.insert(0,symbol)
        formatted_data.append(day_data)
    return formatted_data

results = []

for fut in markets:
    req_url = BASE_URL + f'/markets/{fut}/candles?resolution={RESOLUTION}&limit={LIMIT}'
    res = requests.get(req_url)
    data = res.json()
    data = data["result"]
    for day_data in data:
        data_dict = dict()
        data_dict["Pair"] = fut 
        data_dict["Time"] = timestamp_to_date(day_data["time"])
        data_dict["Open"] = day_data["open"]
        data_dict["High"] = day_data["high"]
        data_dict["Low"] = day_data["low"]
        data_dict["Close"] = day_data["close"]
        results.append(data_dict)

pair_data = getKlineData('BTCUSDT_210625')


with open('ftx_data.csv','w+') as f:
    dict_writer = csv.DictWriter(f, results[0].keys()) #use keys for header
    writer = csv.writer(f)
    dict_writer.writeheader()
    dict_writer.writerows(results)
    writer.writerows(pair_data)
