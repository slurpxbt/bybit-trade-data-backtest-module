import data_pull
import merge_trade_files


data_pull.pull_trade_data("BTCUSD")
data_pull.pull_trade_data("ETHUSD")


# RUN THIS ONLY THE FISRST TIME YOU USE THIS SCRIPT
# this crates first file with trade data after you just update it
# ------------------------------------------------------
# merge_trade_files.merge_all_price_data("BTCUSD")
# merge_trade_files.merge_all_price_data("ETHUSD")
# ------------------------------------------------------



merge_trade_files.append_new_trade_data_files("BTCUSD")
merge_trade_files.append_new_trade_data_files("ETHUSD")