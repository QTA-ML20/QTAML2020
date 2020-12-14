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
daily_factor_collection = mydb[DBConfig.COLLECTION_DAILY_FACTOR]

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
exist_date_list = daily_factor_collection.distinct('datetime')
# 已经存在的就不需要下载了
to_get_date_list = list(set(to_get_date_list) - set(exist_date_list))
to_get_date_list.sort(reverse=True)  # 从最新的日期开始获取

print(f'即将获取{to_get_date_list[-1]}到{to_get_date_list[0]}共{len(to_get_date_list)}天的数据')
# %% 
for i,date in enumerate(to_get_date_list):
    # hs300 = DatabaseReader.get_index_weight('000300.SH', start_date=date,end_date=date).dropna()
    # zz500 = DatabaseReader.get_index_weight('000905.SH', start_date=date,end_date=date).dropna()
    # stock_list = list(pd.concat([hs300, zz500])['code'].unique())
    stock_info = DatabaseReader.get_stock_info(None, date, date)
    stock_info = stock_info[stock_info['expired_date']>date]
    stock_info = stock_info[stock_info['listed_date']<date]
    stock_list = list(stock_info['code'])
    jq_stock_list = [wind2jq(x) for x in stock_list]
    print(f'获取{len(jq_stock_list)}只股票的数据')
    df = get_fundamentals(query(
            valuation
        ).filter(
            # 这里不能使用 in 操作, 要使用in_()函数
            
            valuation.code.in_(jq_stock_list)
        ), date=date)
    print(f'返回{len(df)}只股票的数据')
    del df['id']
    del df['day']
    df['datetime'] = date
    df['code'] = df['code'].apply(jq2wind)
    insert_dataframe(df, daily_factor_collection)
    print(f'{date} 获取完成，目前进度{i+1}/{len(to_get_date_list)}')
    print(get_query_count())
# %%
