import pandas as pd
import numpy as np
from data.config import DBConfig
from pymongo import MongoClient
from functools import lru_cache


class DatabaseObj:
    db_client = MongoClient(DBConfig.ip, username=DBConfig.username, password=DBConfig.password)
    db = db_client[DBConfig.db_name]

    collection_daily_factor = db[DBConfig.COLLECTION_DAILY_FACTOR]
    collection_daily_quote = db[DBConfig.COLLECTION_DAILY_QUOTE]
    collection_daily_style_factor = db[DBConfig.COLLECTION_DAILY_STYLE_FACTOR]
    collection_index_weight = db[DBConfig.COLLECTION_INDEX_WEIGHT]
    collection_minute_quote = db[DBConfig.COLLECTION_MINUTE_QUOTE]
    collection_stock_info = db[DBConfig.COLLECTION_STOCK_INFO]
    collection_trading_days = db[DBConfig.COLLECTION_TRADING_DAYS]


class DatabaseReader:
    # 一个基于数据库直接读取/改写 数据库数据的类
    
    @classmethod
    # @lru_cache(maxsize=1000)
    def get_all_trade_days(cls, start_date=None, end_date=None):
        """
        获取指定日期时间段的交易日期数据，如果没有输入，默认返回所有交易时间段的数据
        :param start_date: 开始日期，如果没有输入
        :param end_date:
        :return:
        """
        query = list()
        if start_date is not None:
            start_date = pd.to_datetime(start_date)
            query.append({"date": {"$gte": start_date}})
    
        if end_date is not None:
            end_date = pd.to_datetime(end_date)
            query.append({"date": {"$lte": end_date}})
    
        collection = DatabaseObj.collection_trading_days
        if len(query) == 0:
            data = list(collection.find({}, {"_id": 0, "date": 1}))
        else:
            data = list(collection.find({"$and": query}, {"_id": 0, "date": 1}))

        if len(data) > 0:
            trading_days = [x.get("date") for x in data]
            # 数据 转换成 DatetimeIndex 类型
            trading_days = pd.DatetimeIndex(trading_days)
            return trading_days
        else:
            fake_data = pd.date_range('2010-01-01', '2020-10-23')
            if start_date is not None:
                fake_data = fake_data[fake_data>=start_date]
            if end_date is not None:
                fake_data = fake_data[fake_data<=end_date]
            return fake_data

    @classmethod
    # @lru_cache(maxsize=1000)
    def get_daily_factor(cls, code_list, factor_list, start_date, end_date):
        """
        从数据库中读取 某一段 时间切片上 特定股票的 日频因子数据
        :param start_date: 输入指定的开始日期
        :param end_date:  输入指定的结束日期
        :param factor_list: 需要请求的那些字段，默认返回所有字段，比如 factor_list = ["PE-TTM", "ST"]
        :param code_list: 返回特定的几只股票的数据
        :return:
        """

        # 将输入的时间转换成 datetime 类型，这样就可以直接和数据库里面的日期 比较
        start_date, end_date = pd.to_datetime(start_date), pd.to_datetime(end_date)
        query = [{"datetime": {'$gte': start_date,
                               '$lte': end_date}}]

        if isinstance(code_list, str):
            code_list = code_list.split(",")

        if code_list is not None:
            # 对于code_list 里面的代码 大写，将 300003.sz 转换成 300003.SZ
            code_list = [x.upper() for x in code_list]
            query.append({"code": {"$in": code_list}})

        # 返回的结果中，只保留查询的指标
        project_field = {"_id": 0, 'datetime': 1, 'code': 1}
        if isinstance(factor_list, str):
            factor_list = factor_list.split(",")
        if factor_list is not None:
            for item in factor_list:
                project_field.update({item: 1})

        collection = DatabaseObj.collection_daily_factor
        db_data = list(collection.find({"$and": query}, project_field))
        if len(db_data) > 0:
            pd_data = pd.DataFrame(db_data)
            return pd_data
        else:
            l = []
            date_list = cls.get_all_trade_days(start_date, end_date)
            for date in date_list:
                fake_df = pd.DataFrame(np.random.rand(len(code_list), len(factor_list)), columns=factor_list)
                fake_df['code'] = code_list
                fake_df['datetime'] = date
                l.append(fake_df)
            fake_data = pd.concat(l)
            return fake_data

    @classmethod
    # @lru_cache(maxsize=1000)
    def get_daily_quote(cls, code_list, start_date, end_date):
        """
        从数据库中读取 某一段 时间切片上 特定股票的 日频因子数据
        :param start_date: 输入指定的开始日期
        :param end_date:  输入指定的结束日期
        :param code_list: 返回特定的几只股票的数据
        :return:
        """

        # 将输入的时间转换成 datetime 类型，这样就可以直接和数据库里面的日期 比较
        start_date, end_date = pd.to_datetime(start_date), pd.to_datetime(end_date)
        query = [{"datetime": {'$gte': start_date,
                               '$lte': end_date}}]

        if isinstance(code_list, str):
            code_list = code_list.split(",")

        if code_list is not None:
            # 对于code_list 里面的代码 大写，将 300003.sz 转换成 300003.SZ
            code_list = [x.upper() for x in code_list]
            query.append({"code": {"$in": code_list}})

        collection = DatabaseObj.collection_daily_quote
        project_field = {"_id": 0}
        db_data = list(collection.find({"$and": query}, project_field))
        if len(db_data) > 0:
            pd_data = pd.DataFrame(db_data)
            return pd_data
        else:
            l = []
            date_list = cls.get_all_trade_days(start_date, end_date)
            for code in code_list:
                fake_df = pd.DataFrame(np.random.rand(len(date_list)), columns=['close'])
                fake_df['close'] = ((np.random.randn(len(date_list))*0.005+1.0003).cumprod()) * 60 * np.random.rand()
                fake_df['open'] = fake_df['close'] + fake_df['close'] * (np.random.rand(len(fake_df)) - 0.5) * 0.02
                fake_df['high'] = fake_df[['close', 'open']].max(axis=1) + fake_df['close'] * np.random.rand(
                    len(fake_df)) * 0.01
                fake_df['low'] = fake_df[['close', 'open']].min(axis=1) - fake_df['close'] * np.random.rand(
                    len(fake_df)) * 0.01
                fake_df['high_limit'] = fake_df['close']
                fake_df['low_limit'] = fake_df['close']
                fake_df['volume'] = (np.random.rand(len(fake_df)) + 1) * 1e8
                fake_df['money'] = (np.random.rand(len(fake_df)) + 1) * 1e9
                fake_df['code'] = code
                fake_df['datetime'] = date_list
                fake_df['paused'] = False
                l.append(fake_df)
            fake_data = pd.concat(l)
            return fake_data

    @classmethod
    # @lru_cache(maxsize=1000)
    def get_daily_style_factor(cls, factor_list, start_date, end_date):
        """
        通过 wss 时间切片的方法，从数据库中读取 某一天 时间切片上所有股票的 相关因子数据
        :param start_date: 输入指定的开始日期
        :param end_date:  输入指定的结束日期
        :param factor_list: 需要请求的那些字段，默认返回所有字段，比如 factor_list = ["PE-TTM", "ST"]
        :return:
        """

        # 将输入的时间转换成 datetime 类型，这样就可以直接和数据库里面的日期 比较
        start_date, end_date = pd.to_datetime(start_date), pd.to_datetime(end_date)
        query = [{"datetime": {'$gte': start_date,
                               '$lte': end_date}}]

        # 返回的结果中，只保留查询的指标
        project_field = {"_id": 0, 'datetime': 1}
        if isinstance(factor_list, str):
            factor_list = factor_list.split(",")
        if factor_list is not None:
            for item in factor_list:
                project_field.update({item: 1})

        collection = DatabaseObj.collection_daily_style_factor
        db_data = list(collection.find({"$and": query}, project_field))
        if len(db_data) > 0:
            pd_data = pd.DataFrame(db_data)
            return pd_data
        else:
            l = []
            date_list = cls.get_all_trade_days(start_date, end_date)
            for date in date_list:
                fake_df = pd.DataFrame(np.random.rand(1, len(factor_list)), columns=factor_list)
                fake_df['datetime'] = date
                l.append(fake_df)
            fake_data = pd.concat(l)
            return fake_data

    @classmethod
    # @lru_cache(maxsize=1000)
    def get_index_weight(cls, index_code, start_date, end_date):
        """
        通过 wss 时间切片的方法，从数据库中读取 某一天 时间切片上所有股票的 相关因子数据
        :param start_date: 输入指定的开始日期
        :param end_date:  输入指定的结束日期
        :param index_code: 股票指数代码，目前只支持000300.XSHG 和 000905.XSHG
        :return:
        """
        if index_code in ["沪深300", "hs300", "300", "HS", "000300.SH"]:
            index_code = "000300.XSHG"

        elif index_code in ["中证500", "zz500", "500", "000905.SH"]:
            index_code = "000905.XSHG"
        assert index_code in ['000300.XSHG', '000905.XSHG']

        # 将输入的时间转换成 datetime 类型，这样就可以直接和数据库里面的日期 比较
        start_date, end_date = pd.to_datetime(start_date), pd.to_datetime(end_date)
        query = [{"datetime": {'$gte': start_date,
                               '$lte': end_date}},
                 {'index_code': index_code}]

        collection = DatabaseObj.collection_index_weight
        project_field = {'_id': 0}
        db_data = list(collection.find({"$and": query}, project_field))
        if len(db_data) > 0:
            pd_data = pd.DataFrame(db_data)
            return pd_data
        else:
            l = []
            date_list = cls.get_all_trade_days(start_date, end_date)
            code_list = [str(i+1).zfill(6)+'.XSHE' for i in range(300)]
            for date in date_list:
                fake_df = pd.DataFrame(np.random.rand(len(code_list)), columns=('weight',))
                fake_df = fake_df/fake_df.sum()  # 归一化
                fake_df['code'] = code_list
                fake_df['datetime'] = date
                l.append(fake_df)
            fake_data = pd.concat(l)
            fake_data['index_code'] = index_code
            return fake_data

    @classmethod
    # @lru_cache(maxsize=1000)
    def get_minute_quote(cls, code_list, start_date, end_date):
        """
        通过 wss 时间切片的方法，从数据库中读取 某一天 时间切片上所有股票的 相关因子数据
        :param start_date: 输入指定的开始日期
        :param end_date:  输入指定的结束日期
        :param code_list: 返回特定的几只股票的数据
        :return:
        """

        # 将输入的时间转换成 datetime 类型，这样就可以直接和数据库里面的日期 比较
        start_date, end_date = pd.to_datetime(start_date), pd.to_datetime(end_date)
        query = [{"date": {'$gte': start_date,
                               '$lte': end_date}}]

        if isinstance(code_list, str):
            code_list = code_list.split(",")

        if code_list is not None:
            # 对于code_list 里面的代码 大写，将 300003.sz 转换成 300003.SZ
            code_list = [x.upper() for x in code_list]
            query.append({"code": {"$in": code_list}})

        collection = DatabaseObj.collection_minute_quote
        project_field = {'_id': 0}
        db_data = list(collection.find({"$and": query}, project_field))
        if len(db_data) > 0:
            pd_data = pd.DataFrame(db_data)
            return pd_data
        else:
            collection = DatabaseObj.collection_minute_quote
            project_field = {'_id': 0}
            db_data = list(collection.find({"$and": query}, project_field))
            pd_data = pd.DataFrame(db_data)
            return pd_data

    @classmethod
    # @lru_cache(maxsize=1000)
    def get_stock_info(cls, code_list, start_date, end_date, field_list=None):
        """
        通过 wss 时间切片的方法，从数据库中读取 某一天 时间切片上所有股票的 相关因子数据
        :param start_date: 输入指定的开始日期
        :param end_date:  输入指定的结束日期
        :param code_list: 返回特定的几只股票的数据
        :return:
        """
        if field_list is not None:
            for field in field_list:
                assert field in ['industry_sw1', 'industry_zx1', 'name', 'listed_date', 'is_st']
        # 将输入的时间转换成 datetime 类型，这样就可以直接和数据库里面的日期 比较
        start_date, end_date = pd.to_datetime(start_date), pd.to_datetime(end_date)
        query = [{"datetime": {'$gte': start_date,
                               '$lte': end_date}}]

        if isinstance(code_list, str):
            code_list = code_list.split(",")

        if code_list is not None:
            # 对于code_list 里面的代码 大写，将 300003.sz 转换成 300003.SZ
            code_list = [x.upper() for x in code_list]
            query.append({"code": {"$in": code_list}})

        # 返回的结果中，只保留查询的指标
        project_field = {"_id": 0, 'datetime': 1, 'code': 1}
        if isinstance(field_list, str):
            field_list = field_list.split(",")
        if field_list is not None:
            for item in field_list:
                project_field.update({item: 1})
        else:
            for item in ['industry_sw1', 'industry_zx1', 'name', 'listed_date', 'is_st']:
                project_field.update({item: 1})

        collection = DatabaseObj.collection_stock_info
        db_data = list(collection.find({"$and": query}, project_field))
        if len(db_data) > 0:
            pd_data = pd.DataFrame(db_data)
            return pd_data
        else:
            l = []
            date_list = cls.get_all_trade_days(start_date, end_date)
            for code in code_list:
                fake_df = pd.DataFrame([False]*len(date_list), columns=['is_st'])
                fake_df['industry_sw1'] = ['申万银行', '申万计算机', '申万通信'][int(np.ceil(np.random.rand() * 3) - 1)]
                fake_df['industry_zx1'] = ['中信银行', '中信计算机', '中信通信'][int(np.ceil(np.random.rand() * 3) - 1)]
                fake_df['name'] = ['视觉中国', '中国建筑', '海康威视'][int(np.ceil(np.random.rand() * 3) - 1)]
                fake_df['listed_date'] = pd.to_datetime('1994-09-27')
                fake_df['code'] = code
                fake_df['datetime'] = date_list
                l.append(fake_df)
            fake_data = pd.concat(l)
            return fake_data

    @classmethod
    def update_daily_factor(cls, df, factor_name):
        """
                更新特定股票特定时间的因子值
                :param df: 有datetime、code和factor_name 三列数据的dataframe
                :param factor_name:  待更新的因子值的名称
                :return:
        """
        assert isinstance(df, pd.DataFrame)
        assert 'datetime' in df.columns
        assert 'code' in df.columns
        assert factor_name in df.columns

        record_list = df[['datetime', 'code', factor_name]].to_dict('record')
        collection = DatabaseObj.collection_daily_factor
        for record in record_list:
            collection.update_one({'datetime': record['datetime'],
                                   'code': record['code']},
                                  {'$set': {
                                      factor_name: record[factor_name]
                                  }})

    @classmethod
    def deleted_daily_factor(cls, factor_name, start_date=None, end_date=None):
        """
                更新特定股票特定时间的因子值
                :param start_date: 输入指定的开始日期
                :param end_date:  输入指定的结束日期
                :param factor_name:  待删除的因子值的名称
                :return:
        """
        query = []
        if start_date is not None and end_date is not None:
            assert end_date >= start_date
            query.append({'datetime': {
                '$gte': start_date,
                '$lte': end_date
            }})

        collection = DatabaseObj.collection_daily_factor
        collection.update_many({'$and': query},
                              {'$unset': {
                                  factor_name: ""
                              }})


if __name__ == "__main__":
    a=DatabaseReader.get_minute_quote(['000001.XSHE','000002.XSHE'],
                                     '2020-01-01', '2020-01-05')

