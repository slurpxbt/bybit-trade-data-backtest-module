import pickle
import pandas as pd
import datetime as dt
import traceback
import binance_candle_data as bcd


def get_emas(pair, time_frame):
    candles = bcd.get_candle_data(pair, time_frame, dt.datetime.today()-dt.timedelta(days=120), dt.datetime.today())
    candles["EMA21_high"] = round(candles["high"].ewm(span=21).mean(), 2)
    candles["EMA21_low"] = round(candles["low"].ewm(span=21).mean(), 2)
    candles["EMA21_close"] = round(candles["close"].ewm(span=21).mean(), 2)
    print(candles)
    del candles["open"]
    del candles["volume"]
    del candles["number_of_trades"]
    del candles["open_time"]
    del candles["close_time"]


    return candles



folder_path = "YOUR FOLDER PATH"

ticker = "BTCUSD"


# load data
price_data = pickle.load(open(f"{folder_path}/price_data/{ticker}.p", "rb"))
price_data["timestamp"] = pd.to_datetime(price_data["timestamp"], format='%d%b%Y:%H:%M:%S')
last_row = pd.to_datetime(price_data.iloc[-1]["timestamp"])




# TEST PERIOD
test_period_months = {"first": 7, "last":1}
test_period_years = {"first": 2020, "last":2021}

# -----------------------------------------------------------------

start_date = dt.datetime(test_period_years["first"], test_period_months["first"], 1)
end_date = dt.datetime(test_period_years["last"], test_period_months["last"], 1)


num_months = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
start = dt.datetime(test_period_years["first"], test_period_months["first"], 2)

new_date = start
intervals = []
interval = []

# backtest intervals
for i in range(1, num_months+1):

    # creates intervals between first and last date(intervals of 1 month)
    interval.append(new_date)
    if new_date < last_row:
        print(new_date)

        new_date = start + dt.timedelta(days=i*365/12)
        interval.append(new_date)
    else:
        print("date out of price data")

    if new_date < last_row:
        intervals.append(interval)
        interval = []



# interval data
for i in intervals:
    print("-"*100)
    print("NEW INTERVAL")
    print("-"*100)
    print(f"start: {i[0]} end: {i[1]}, {type(i[0])} {type(i[1])}")
    
    try:
        # select data within the wanted interval
        data = price_data.loc[(price_data['timestamp'] >= i[0]) & (price_data['timestamp'] <= i[1])]
        print(data)
        data.sort_values(by=['timestamp'], ascending=True, inplace=True)
        print(data)
        data_list = data.values.tolist()


        input("STOP")
        emas = get_emas("BTCUSDT", "4h")
        print(emas)
        
        # STRATEGY
        close_ = False
        close_hour = ""
        for index in range(len(data_list)):  # len(data_list)
            
            row = data_list[index]
            #print(row[0].year, row[0].month, row[0].day, row[0].hour, row[0].minute, row[0].second )

            # ---------------------------------------------------------------------------
            #4H closes:
            # # make it so you get only one number for 4h closes
            # closes_4h = [0, 4, 8, 12, 16, 20]
            # if row[0].hour in closes_4h and row[0].minute == 0 and close_ == False: 
            #     if close_hour == row[0].hour:
            #         pass
            #     else:
            #         close_ = True
            #         close_hour = row[0].hour
            #         print(row)
            # else:
            #     close_ = False
            
            # ---------------------------------------------------------------------------
            # 1H closes
            # closes_1h = range(0,24)
            # if row[0].hour in closes_1h and row[0].minute == 0 and close_ == False: 
            #     if close_hour == row[0].hour:
            #         pass
            #     else:
            #         close_ = True
            #         close_hour = row[0].hour
            #         print(row)
            # else:
            #     close_ = False

            # 15min closes
            closes_15min = [0, 15, 30, 45]
            if row[0].minute in closes_15min and close_ == False: 
                if close_hour == row[0].minute:
                    pass
                else:
                    close_ = True
                    close_hour = row[0].minute
                    print(row)
            else:
                close_ = False

            

            

    except Exception as e:
        print(e)
        traceback.print_exc()
