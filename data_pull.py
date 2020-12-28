import datetime as dt
import pandas as pd
import os
import json
import requests
import time
import gzip
import io
import urllib.request

#2019-10-01
#2020-10-02




def pull_trade_data(ticker):


    folder_path = "YOUR FILE PATH"

    url1 = f'https://public.bybit.com/trading/{ticker}/{ticker}'

    dates_tmp = []
    tempDate = dt.datetime(2019,10,1).date()
    dates_tmp.append(str(tempDate))

    while tempDate < dt.datetime.today().date() - dt.timedelta(days=1): 
        tempDate = tempDate + dt.timedelta(days=1)
        dates_tmp.append(str(tempDate))

    # implement check for missing dates: and pull only missing files

    files = os.listdir(f"{folder_path}/data/{ticker}")
   
    dates = []
    for date in dates_tmp:
        if date not in files:
            #dates.remove(date)
            print(date)
            dates.append(date)

    print(f"missing files for {ticker}")
    print(dates)


    if len(dates) != 0:
        for i in range(len(dates)):
            OUTFILE_PATH = f'{folder_path}/data/{ticker}/' + dates[i]
            s = time.time()
            url = url1 + dates[i] + '.csv.gz'
            response = urllib.request.urlopen(url)
            compressed_file = io.BytesIO(response.read())
            decompressed_file = gzip.GzipFile(fileobj=compressed_file)


            with open(OUTFILE_PATH, 'wb') as outfile:
                outfile.write(decompressed_file.read())
            print(dates[i],'Completed in ',round(time.time()-s,2),'seconds.')

    else:
        print(f"no missing data for {ticker}")




pull_trade_data("BTCUSD")
print("-----------")
pull_trade_data("ETHUSD")

