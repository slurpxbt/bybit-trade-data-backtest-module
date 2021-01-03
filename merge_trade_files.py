
import datetime as dt
import pandas as pd
import os
import json
import requests
import time
import gzip
import io
import urllib.request
import pickle
# ----------------------------------------------------------------------------------

# print all data for ticker


def merge_all_price_data(ticker):


    folder_path = "your folder path"


    print("merging.....")
    start_date = dt.datetime(2019,10,1)

    #start_date = dt.datetime(2020,12,20)
    end_date = dt.datetime.today()
    file_exist = True


    price_data = pd.DataFrame(columns=["timestamp", "symbol", "side", "size", "price"])
    while file_exist == True:

        file_path = f"{folder_path}/data/{ticker}/{start_date.date()}"
        last_file_path = f"{folder_path}/data/{ticker}/{end_date.date()}"

        if os.path.exists(file_path):

            if file_path <= last_file_path:
                bybit1 = pd.read_csv(file_path,parse_dates=True)
                bybit1['timestamp'] = pd.to_datetime(bybit1['timestamp'],unit='s')

                bybit1.drop(["tickDirection", "trdMatchID", "grossValue", "homeNotional", "foreignNotional"], axis=1, inplace=True)

                #print(bybit1)
                price_data = price_data.append(bybit1)

            else:
                print("interval printed")
                break
        else:
            print("End data")
            file_exist = False

        start_date = start_date + dt.timedelta(days=1)
        
    print(price_data)

    print("dumping....")
    pickle.dump(price_data, open(f"{folder_path}/price_data/{ticker}.p", "wb"))


#merge_all_price_data("BTCUSD")
#merge_all_price_data("ETHUSD")




def append_new_trade_data_files(ticker):
    folder_path = "your folder path"

    file_path = f"{folder_path}/price_data/{ticker}.p"

    price_data = pickle.load(open(file_path, "rb"))
    
    print(price_data)

    last_date = price_data.iloc[-1]["timestamp"].date()
    last_day = last_date.day
    last_month = last_date.month
    last_year = last_date.year

    dates_tmp = []
    tempDate = dt.datetime(last_year,last_month,last_day).date() + dt.timedelta(days=1)
    #dates_tmp.append(str(tempDate))
   

    if tempDate != dt.datetime.now().date():
        while tempDate < dt.datetime.today().date() - dt.timedelta(days=1):
            tempDate = tempDate + dt.timedelta(days=1)
            dates_tmp.append(str(tempDate))

    if len(dates_tmp) > 0:
        print(dates_tmp)
        for day in dates_tmp:
            file_path = f"{folder_path}/data/{ticker}/{day}"

            if os.path.exists(file_path):
                data = pd.read_csv(file_path,parse_dates=True)
                data['timestamp'] = pd.to_datetime(data['timestamp'],unit='s')

                data.drop(["tickDirection", "trdMatchID", "grossValue", "homeNotional", "foreignNotional"], axis=1, inplace=True)

                price_data = price_data.append(data)

        print(price_data)
        print("dumping....")
        pickle.dump(price_data, open(f"{folder_path}/price_data/{ticker}.p", "wb"))
    else:
        print("no missing data")

# append_new_trade_data_files("BTCUSD")
# append_new_trade_data_files("ETHUSD")