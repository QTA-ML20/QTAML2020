# -*- coding: utf-8 -*-
"""
Created on Sun Oct 25 12:53:05 2020

@author: YeWenxuan
"""
import sys
sys.path.append('..')

from jqdatasdk import *
from config import *
import pandas as pd
import numpy as np
import pymongo
import json
import datetime


# 本脚本的参数，限定获取数据的时间范围与股票范围
start_date = '2020-01-01'
end_date = '2020-12-31'

index_list = ['000300.XSHG', '000905.XSHG']  # 暂时只下载沪深300和中证500成分股的数据

collection_name = 'minute_quote'
# 登录Joinquant
auth(JQDataConfig.username,JQDataConfig.password)

# 登录 MongoDB
myclient = pymongo.MongoClient(DBConfig.ip, username=DBConfig.username, 
                               password=DBConfig.password)
mydb = myclient[DBConfig.db_name]
mycollection = mydb[collection_name]

def insert_dataframe(df, collection):
    collection.insert_many(df.to_dict('record'))

# 获取start date 到 end date 之间的交易日
trading_date_array = get_trade_days(start_date=start_date, end_date=end_date)
now = datetime.datetime.now()
if now.time().hour >= 17:
    # 如果现在时间大于17点，则当天数据也要下载
    trading_date_array = trading_date_array[trading_date_array<=now.date()]
else:
    # 不然下载到昨天就可以了
    trading_date_array = trading_date_array[trading_date_array<now.date()]
    
    
trading_date_list = [pd.to_datetime(x) for x in trading_date_array]    
exist_date_list = mycollection.distinct('date')
to_download_date_list = list(set(trading_date_list) - set(exist_date_list))
to_download_date_list.sort()
to_download_date_count = len(to_download_date_list)
print(f'将获取 {to_download_date_list[0]} ~ {to_download_date_list[-1]} 共 {to_download_date_count} 天的数据')



# 逐个交易日获取数据
for i, trading_date in enumerate(to_download_date_list): 
    
    # 获取每个指数当天的成分股
    stock_list = []
    for index in index_list:
        stocks = get_index_stocks(index)
        stock_list = stock_list + stocks
    # 去重
    stock_list = list(set(stock_list))
    stock_list.sort()
    stock_count = len(stock_list)
    print(f'总共要获取 {stock_count} 只股票的数据')
    
    # 获取分钟数据：后复权与不复权皆有、停牌时数据为NaN
    trading_date_end = pd.to_datetime(trading_date.strftime('%Y-%m-%d') + ' 15:00:00')  # 当天收盘时间
    # 先取后复权的数据
    post_df = get_price(stock_list, 
                        start_date=trading_date, 
                        end_date=trading_date_end, 
                        frequency='minute', fields=None, skip_paused=False, 
                        fq='post', panel=False, fill_paused=False)
    # 再取不复权的数据
    raw_df = get_price(stock_list, 
                        start_date=trading_date, 
                        end_date=trading_date_end, 
                        frequency='minute', fields=['open', 'high', 'low', 'close'], skip_paused=False, 
                        fq=None, panel=False, fill_paused=False)
    raw_df.rename(columns={'open': 'raw_open',
                           'high': 'raw_high',
                           'low': 'raw_low',
                           'close': 'raw_close'}, inplace=True)
    # 拼接在一起
    df = pd.merge(post_df, raw_df, on=['time', 'code'])
    df['date'] = pd.to_datetime(trading_date)
    
    # 插入数据库
    insert_dataframe(df, mycollection)
    
    
    print(f'{trading_date} 获取完成，目前进度{i+1}/{to_download_date_count}')
    print(get_query_count())


    