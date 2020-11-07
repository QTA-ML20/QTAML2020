# -*- coding: utf-8 -*-
"""
Created on Sun Oct 25 14:44:31 2020

@author: LENOVO
"""

import pandas as pd
import numpy as np
from scipy import stats
from data.DBReader import DatabaseReader as dbr



class SameClassFacAna():
    
#    def __init__(self, class_of_factor, key, df, p_value=0.05, retro=60):
#        self.p_value = p_value
#        self.class_of_factor = class_of_factor
#        self.key = key
#        self.retro = retro
#        self.df = df
    @classmethod
    def factor_compose(cls,factor1, factor2, method='IC'):
        '''
        同类因子合成
        :param: factor1: 需要合成的因子1
        :param: factor2: 需要合成的因子2
        :param: method：选用的合成方法
        '''
    #IC&IR的矩阵 code*datetime
        pass
    
    @classmethod
    def cor_test(cls, class_of_factor, key, df, p_value=0.05, retro=60):
        '''
        同类因子相关性检验
        :param: p_value:相关性检验所用显著性水平,如果没有输入，默认为0.05
        :param: class_of_factor: 记载因子分类的字典
        :param: key:所关心的因子类别
        :param: retro:相关性检验所用数据回溯期数，如果没有输入，默认为60
        :param: df:包含股票，日期及因子暴露的数据
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
    df = dbr.get_daily_factor(stock_list,factor_list,start_date,end_date)
    results = []
    for key in class_of_factor.keys():
        results.append(SameClassFacAna.cor_test(class_of_factor, key, df, 0.05, 60))
    
    #有显著相关性的进入因子合成
    for result in results:
        for i in range(len(result)):
            for j in range(i+1,len(result)):
                if(result[i,j]<p_value):
                    SameClassFacAna.factor_compose(result[i],result[j],'IC')