
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





def merge_price_data(ticker):


    folder_path = "YOUR FOLDER PATH"


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


merge_price_data("BTCUSD")
merge_price_data("ETHUSD")