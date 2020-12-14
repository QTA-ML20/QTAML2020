# %% 
import sys
sys.path.append('..')

from jqdatasdk import *
from config import *
import pandas as pd
import numpy as np
import pymongo
import json
import datetime
from jqdatasdk import *
from DBReader import DatabaseReader
# 登录Joinquant
auth(JQDataConfig.username,JQDataConfig.password)
print(get_query_count())

# 登录 MongoDB
myclient = pymongo.MongoClient(DBConfig.ip, username=DBConfig.username, 
                               password=DBConfig.password)
mydb = myclient[DBConfig.db_name]
stock_info_collection = mydb[DBConfig.COLLECTION_STOCK_INFO]
daily_quote_collection = mydb[DBConfig.COLLECTION_DAILY_QUOTE]

def insert_dataframe(df, collection):
    collection.insert_many(df.to_dict('record'))

def wind2jq(code):
    if code.endswith('.SH'):
        return code.split('.')[0] + ".XSHG"
    elif code.endswith(".SZ"):
        return code.split('.')[0] + ".XSHE"
    else:
        print(code+"代码格式错误")
        return None

def jq2wind(code):
    if code.endswith('.XSHG'):
        return code.split('.')[0] + ".SH"
    elif code.endswith(".XSHE"):
        return code.split('.')[0] + ".SZ"
    else:
        print(code+"代码格式错误")
        return None

# 获取待爬取的日期序列
to_get_date_list = stock_info_collection.distinct('datetime')
to_get_date_list.sort()

index_code_list = ['000300.SH', '000905.SH']
for index_code in index_code_list:
    df = get_price(wind2jq(index_code), to_get_date_list[0], to_get_date_list[-1]).dropna()
    df['pre_close'] = df['close'].shift(1)
    df['ret'] = df['close']/df['pre_close'] - 1
    df['code'] = index_code
    df['datetime'] = pd.DatetimeIndex(df.index)
    df.reset_index(inplace=True, drop=True)
    insert_dataframe(df, daily_quote_collection)
# %%
