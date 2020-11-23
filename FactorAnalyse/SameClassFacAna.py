# -*- coding: utf-8 -*-
"""
Created on Sun Oct 25 14:44:31 2020

@author: LENOVO
"""

import pandas as pd
import numpy as np
from scipy import stats
import sys
sys.path.append('..')
from data.DBReader import DatabaseReader as dbr
from MultiFactorTest import FactorTestOneDay, get_IC_FactorReturn


class SameClassFacAna():
    
#    def __init__(self, class_of_factor, key, df, p_value=0.05, retro=60):
#        self.p_value = p_value
#        self.class_of_factor = class_of_factor
#        self.key = key
#        self.retro = retro
#        self.df = df
    @classmethod
    def factor_compose(cls, factor1, factor2, factor_df, return_df, method='IC'):
        '''
        同类因子合成
        :param: factor1: 需要合成的因子1
        :param: factor2: 需要合成的因子2
        :param: method：选用的合成方法,默认值为IC
        :return: 返回以IC值为权重加权合成的新因子值(dataframe)
        '''
        if method == 'IC':
            factor_df = factor_df[['code', factor1, factor2]] #取对应因子的数据
            stock_return_df = return_df[['code','ret']] # 取股票的return
            # 转换为每日一个dataframe的形式
            factor_date_dict = dict(list(factor_df.groupby('date',as_index=False)))
            stock_return_shift_df = stock_return_df.iloc[1:,:] # 往后推一天
            # 转换为每日一个dataframe的形式
            stock_return_shift_dict = dict(list(stock_return_shift_df.groupby('date',as_index=False)))
            # 计算IC值
            IC_df_1,factor_return_df = get_IC_FactorReturn(factor_date_dict,stock_return_shift_dict)
            IC_df_1.rename(columns={'factor1': 'weight1', 'factor2': 'weight2'}, inplace=True)
            # 合并并用IC值加权
            factor_compose = pd.merge(factor_df, IC_df_1, on='code')
            composed_factor = factor_compose['factor1']*factor_compose['weight1']
                             + factor_compose['factor2']*factor_compose['weight2']
            
        return composed_factor
    
    @classmethod
    def cor_test(cls, class_of_factor, key, df, retro=60):
        '''
        同类因子相关性检验
        :param: class_of_factor: 记载因子分类的字典
        :param: key:所关心的因子类别
        :param: retro:相关性检验所用数据回溯期数，如果没有输入，默认为60
        :param: df:包含股票，日期及因子暴露的数据
        :return: 返回计算出的各因子相关p值矩阵（上三角）
        '''
        factors = class_of_factor[key]
        remains = factors+['datetime','code']
        dates = list(set(df['datetime']))
        dates.sort()
        original = df[remains]
        s_date = dates[retro*-1]
        df_curr = original[(original['datetime'] >= s_date)]
        time_groups = df_curr.groupby('datetime')
    
        times = []
        for date in dates[retro*-1:]:
            temp = time_groups.get_group(date).set_index('code')
            temp.drop(columns = ['datetime'],inplace=True)
            times.append(temp)
            
        results = []
        for time in times:
            temp = time.corr()
            results.append(temp)
            
        total_mean = pd.DataFrame(index=factors,columns=factors)
        total_mean.loc[:,:] = 0
        
        #所有相关性均值计算
        for result in results:
            total_mean += result
        total_mean /= retro
        
        #相关性标准差计算
        total_std = pd.DataFrame(index=factors,columns=factors)
        total_std.loc[:,:] = 0
        for i in range(len(factors)):
            for j in range(i+1,len(factors)):
                temp = []
                for result in results:
                    temp.append(result.iat[i,j])
                total_std.loc[factors[i],factors[j]] = np.std(temp)
        total_std /= np.sqrt(retro-1)
        total_t = total_mean/total_std
        total_p=2*(1-stats.t.cdf(total_t,retro-1))
        return total_p
    
    @classmethod
    def cor_factor(cls,class_of_factor, key, total_p, p_value=0.05):
        '''
        输出相关性显著的因子对
        :params: class_of_factor:字典，记录因子分类情况
        :params: key：关心的因子类型
        :params: total_p:记录各因子对之间相关p值的矩阵
        :params: p_value:检验所用显著性水平，默认值为0.05
        :return: 返回显著相关的因子对(factor1, factor2)
        '''
        result=[]
        factors=class_of_factor[key]
        for i in range(len(total_p)):
            for j in range(i+1,len(total_p)):
                if(total_p[i,j]<p_value):
                    result.append([factors[i],factors[j]])
        return result
    
if __name__ == '__main__':
    #因子分类字典，后续更新
    class_of_factor = {'class1':['factor'+str(i+1) for i in range(5)],
                     'class2':['factor'+str(i+1) for i in range (5,10)]}
    start_date = '2020-01-01'
    end_date = '2020-03-31'
    p_value=0.05
    stock_list = list(dbr.get_index_weight('000300.XSHG',start_date, end_date)['code'])
    #假设是这些因子，有数据再改
    factor_list = ['factor'+str(i+1) for i in range (10)]
    # 获取因子数据
    factor_df = dbr.get_daily_factor(stock_list,factor_list,start_date,end_date)
    # 获取股票return
    stock_return_df = dbr.get_daily_quote(stock_list,start_date,end_date)
    for key in class_of_factor.keys():
        resultp = SameClassFacAna.cor_test(class_of_factor, key, factor_df, 60)
        results = SameClassFacAna.cor_factor(class_of_factor, key, resultp, 0.05)
        if(len(results)!=0):
            for result in results:
                # 因子合成
                SameClassFacAna.factor_compose(result[0],result[1],factor_df,stock_return_df,method='IC')
            
