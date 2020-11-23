# %%
import pandas as pd
import numpy as np
import pymongo
import json
import datetime
import os
from config import *
# from jqdatasdk import *
# 登录Joinquant
# auth(JQDataConfig.username,JQDataConfig.password)

# login db
myclient = pymongo.MongoClient(DBConfig.ip, username=DBConfig.username, 
                               password=DBConfig.password)
mydb = myclient[DBConfig.db_name]

stock_code = pd.read_csv(data_dir+"STOCK_CODE.csv", encoding="utf-8", header=None)
stock_name = pd.read_csv(data_dir+"STOCKNAME.csv", encoding="ansi", header=None)
all_trade_days = pd.DatetimeIndex(pd.read_csv(data_dir+"ALLDATES.csv",header=None).iloc[:, 0].astype(str))
dt_list = pd.read_csv(data_dir+"DT_LIST.csv", encoding="utf-8", header=None)
industry_name = pd.read_csv(data_dir+"INDUSTRYNAME.csv", encoding="ansi", header=None)


quote_series_list = ["ADJFACTOR", "AMOUNT", "OPEN", "HIGH", "LOW", "CLOSE",
                     "PRECLOSE", "RETURN", "VOLUME", "ISST", "ISTRADEDAY",
                     "SIZE", "STOCK_POOL", "WEIGHT_000300_SH", "WEIGHT_000905_SH", "ZX_1"]
for f in quote_series_list:
        # try:
        if f == 'STOCK_POOL':
                exec(f"{f} = pd.read_csv('{data_dir+f}.csv', encoding='utf-8')")
                exec(f"{f}.set_index('0', inplace=True)")
        else:
                exec(f"{f} = pd.read_csv('{data_dir+f}.csv', encoding='utf-8', header=None)")
                exec(f"{f}.set_index(0, inplace=True)")
        exec(f"{f}.index = pd.DatetimeIndex({f}.index.astype(str))")
        exec(f"{f}.columns = stock_code.iloc[:,0].to_list()")
        # except:
        #     exec(f"{f} = pd.read_csv('{data_dir + f}.csv', encoding='ansi', header=None)")
        print(f'data file {f} loaded.')

print("loading done!")

# %% stock info
melted_industry_zx1 = ZX_1.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"industry_zx1", "variable":"code"})
melted_is_st = ISST.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"is_st", "variable":"code"})
stock_info_df = pd.merge(melted_industry_zx1, melted_is_st, on=['code', 'datetime'])

stock_name_dict = stock_name.set_index(0)[1].to_dict()
stock_info_df['name'] = stock_info_df['code'].map(stock_name_dict)

listed_date_dict = dt_list.set_index(0)[1].astype(str).to_dict()
expired_date_dict = dt_list.set_index(0)[2].astype(str).replace({'30001231':'22620411'}).to_dict()
stock_info_df['listed_date'] = pd.DatetimeIndex(stock_info_df['code'].map(listed_date_dict))
stock_info_df['expired_date'] = pd.DatetimeIndex(stock_info_df['code'].map(expired_date_dict))

industry_code_dict = industry_name[industry_name[3]=='zx_1'].set_index(2)[0].to_dict()
industry_name_dict = industry_name[industry_name[3]=='zx_1'].set_index(2)[1].to_dict()
stock_info_df['industry_zx1_code'] = stock_info_df['industry_zx1'].map(industry_code_dict)
stock_info_df['industry_zx1_name'] = stock_info_df['industry_zx1'].map(industry_name_dict)

stock_info_df['is_st'] = stock_info_df['is_st'].map({0:False, 1:True})

# insert to database
print('start to insert stock info')
collection = mydb[DBConfig.COLLECTION_STOCK_INFO]
collection.insert_many(stock_info_df.to_dict('record'))
del stock_info_df
print('stock info done!')

# %% daily quote 
melted_open = OPEN.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"raw_open", "variable":"code"})
melted_high = HIGH.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"raw_high", "variable":"code"})
melted_low = LOW.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"raw_low", "variable":"code"})
melted_close = CLOSE.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"raw_close", "variable":"code"})
melted_volume = VOLUME.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"raw_volume", "variable":"code"})
melted_amount = AMOUNT.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"money", "variable":"code"})
melted_preclose = PRECLOSE.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"pre_close", "variable":"code"})
melted_ret = RETURN.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"ret", "variable":"code"})
melted_trading_status = ISTRADEDAY.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"paused", "variable":"code"})
melted_adjfactor = ADJFACTOR.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"adj_factor", "variable":"code"})

melted_trading_status['paused'] = melted_trading_status['paused'].map({1:False, 0:True, np.NaN:True})

l = [melted_open, melted_high, melted_low, melted_close, melted_volume, melted_amount,
     melted_preclose, melted_ret, melted_trading_status, melted_adjfactor]
daily_quote_df = pd.concat([temp_df.iloc[:,2] for temp_df in l], axis=1)
daily_quote_df = pd.concat([melted_open[['datetime', 'code']],daily_quote_df], axis=1)

daily_quote_df['name'] = daily_quote_df['code'].map(stock_name_dict)
daily_quote_df['open'] = daily_quote_df['raw_open'] * daily_quote_df['adj_factor']
daily_quote_df['high'] = daily_quote_df['raw_high'] * daily_quote_df['adj_factor']
daily_quote_df['low'] = daily_quote_df['raw_low'] * daily_quote_df['adj_factor']
daily_quote_df['close'] = daily_quote_df['raw_close'] * daily_quote_df['adj_factor']
daily_quote_df['volume'] = daily_quote_df['raw_volume'] / daily_quote_df['adj_factor']

# insert to database
print('start to insert daily quote')
collection = mydb[DBConfig.COLLECTION_DAILY_QUOTE]
collection.insert_many(daily_quote_df.to_dict('record'))
del daily_quote_df
print('daily quote done!')

# %% index_weight
# 300
melted_weight_300 = WEIGHT_000300_SH.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"weight", "variable":"code"})
melted_weight_300 = melted_weight_300[melted_weight_300['weight']!=0.].reset_index(drop=True).sort_values("datetime")
melted_weight_300['index_code'] = '000300.SH'
# 500
melted_weight_500 = WEIGHT_000905_SH.reset_index().melt(id_vars=0).rename(columns={0:"datetime", 
"value":"weight", "variable":"code"})
melted_weight_500 = melted_weight_500[melted_weight_500['weight']!=0.].reset_index(drop=True).sort_values("datetime")
melted_weight_500['index_code'] = '000905.SH'

index_weight_df = pd.concat([melted_weight_300, melted_weight_500], axis=0)

# insert to database
print('start to insert index weight')
collection = mydb[DBConfig.COLLECTION_INDEX_WEIGHT]
collection.insert_many(index_weight_df.to_dict('record'))
print('index weight done!')

# %% trading_days
trading_days_df = pd.DataFrame(all_trade_days.values, columns=('date',))

# insert to database
print('start to insert trading days')
collection = mydb[DBConfig.COLLECTION_TRADING_DAYS]
collection.insert_many(trading_days_df.to_dict('record'))
print('trading days done!')



