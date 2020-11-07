# -*- coding: utf-8 -*-
"""
Created on Sun Oct 25 14:44:31 2020

@author: LENOVO
"""

import pandas as pd
import numpy as np
from scipy import stats
import pdb

# 捏的股票因子数据
stock_list = [str(i+1).zfill(6)+'.sz' for i in range(300)]
date_list = pd.date_range('2020-01-01', '2020-03-31')
factor_list = ['factor'+str(i+1) for i in range (10)]
l = []

for date in date_list:
    df = pd.DataFrame(np.random.rand(len(stock_list), len(factor_list)), columns=factor_list)
    df['datetime'] = date
    df['code'] = stock_list
    l.append(df)  
df = pd.concat(l)


def factor_compose(i,j):
    #IC&IR的矩阵 code*datetime
    pass

#因子分类(先假设这么分)
class_of_factor={'class1':['factor'+str(i+1) for i in range(5)],
                           'class2':['factor'+str(i+1) for i in range (5,10)]}

#class FactorCorTest():
#    '''
#    同类因子相关性检验
#    '''
#    
#    def __init__(self, p_value, class_of_factor, retro, df):
#        self.p_value = p_value
#        self.class_of_factor = class_of_factor
#        self.retro = retro
#        self.df = df
#        
#    def cor_test(self, p_value, class_of_factor, retro, df):
#        for key in class_of_factor.keys():
#            factors = class_of_factor[key]
#            remains = factors+['datetime','code']
#            dates = list(set(df['datetime']))
#            dates.sort()
#            original = df[remains]
#            s_date = dates[retro*-1]
#            df_curr = original[(original['datetime'] >= s_date)]
#            time_groups = df_curr.groupby('datetime')
#        #        time_std = time_groups.corr()
#        
#            times = []
#            for date in dates[retro*-1:]:
#                temp = time_groups.get_group(date).set_index('code')
#                temp.drop(columns = ['datetime'],inplace=True)
#                times.append(temp)
#                
#            results = []
#            for time in times:
#                temp = time.corr()
#                results.append(temp)
#                
#            total_mean = pd.DataFrame(index=factors,columns=factors)
#            total_mean.loc[:,:] = 0
#            
#            #所有相关性均值计算
#            for result in results:
#                total_mean += result
#            total_mean /= retro
#            
#            #相关性标准差计算
#            total_std = pd.DataFrame(index=factors,columns=factors)
#            total_std.loc[:,:] = 0
#            for i in range(len(factors)):
#                for j in range(i+1,len(factors)):
#                    temp = []
#                    for result in results:
#                        temp.append(result.iat[i,j])
#                    total_std.loc[factors[i],factors[j]] = np.std(temp)
#            total_std /= np.sqrt(retro-1)
#            total_t =total_mean/total_std
#            total_p=2*(1-stats.t.cdf(total_t,retro-1))
#            
#            #有显著相关性的进入因子合成
#            for i in range(len(factors)):
#                for j in range(i+1,len(factors)):
#                    if(total_p[i,j]<p_value):
#                        factor_compose(factors[i],factors[j])
#
#def main():
#    CorTest = FactorCorTest(0.05, class_of_factor, 60, df)
#    
#if __name__ == '__main__':
#    main()

p_value = 0.05
retro = 60
for key in class_of_factor.keys():
    factors = class_of_factor[key]
    remains = factors+['datetime','code']
    dates = list(set(df['datetime']))
    dates.sort()
    original = df[remains]
    s_date = dates[retro*-1]
    df_curr = original[(original['datetime'] >= s_date)]
    time_groups = df_curr.groupby('datetime')
#        time_std = time_groups.corr()

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
    total_t =total_mean/total_std
    total_p=2*(1-stats.t.cdf(total_t,retro-1))
    
    #有显著相关性的进入因子合成
    for i in range(len(factors)):
        for j in range(i+1,len(factors)):
            if(total_p[i,j]<p_value):
                factor_compose(factors[i],factors[j])
                
