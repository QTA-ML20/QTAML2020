# -*- coding: utf-8 -*-
"""
Created on Sat Nov  7 15:20:42 2020

@author: Mengjie Ye
"""
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys
import statsmodels.api as sm
from scipy import stats
path_wx='d://QTA/project//code//qtaYWX'
path_preprocessing ='d://QTA/project//code//PreProcessing'
sys.path.append(path_wx)
sys.path.append(path_preprocessing)

from FactorAnalyse.DataPreProcessing import *
from data.DBReader import DatabaseReader
from PreProcessing import get_daily_factor_preprocessed
#%%

#假设已经有了算好的因子字典
#key是date，value是factor_date_df
Factor_date_dict = {'20200101':pd.DataFrame(np.random.random((300,5)))}
date='20200101'
factor_date_df = Factor_date_dict[date]

#%%
class FactorTestOneDay(factor_date_df,stock_return_shift_se,date):
    '''
    输入一期的未处理的原始factor，滞后T期的stock_return，factor那天的日期
    进行factor的预处理，每日IC和factor return的计算
    
    '''
    def __init__(self):
        self.factor_date_df = factor_date_df
        self.stock_return_shift_se = stock_return_shift_se
        self.date = date
        
        
    def preprocessing_one_day(self):
        factor_date_df = self.factor_date_df
        #去极值
        ff = factor_date_df.apply(DeExtremeMethod.Method_Mean_Std,axis=0)
        #中性化
        ff_neu = ff.apply(NeutralizeMethod.Method_Residual,control_factor_df=control_factor_date,axis=0)
        #Zscore
        factor_date_preprocessed_df = ff_neu.apply(StandardizeMethod.Method_Z_Score)
        return factor_date_preprocessed_df
    
    def get_IC_one_day(self):
        factor_date_preprocessed_df = self.preprocessing_one_day()
        stock_return_shift_se = self.stock_return_shift_se
        date = self.date
        ic_one_day=pd.DataFrame(index=[date],columns=factor_date_preprocessed_df.columns)
        for i in range(factor_date_preprocessed_df.shape[1]):
            ic_one_day.iloc[0,i]=np.corrcoef(factor_date_preprocessed_df.iloc[:,i],stock_return_shift_se)[0,1]
        return ic_one_day
    
    def get_factor_return_one_day(self):
        factor_date_preprocessed_df = self.preprocessing_one_day()
        stock_return_shift_se = self.stock_return_shift_se
        date = self.date
        factor_return_one_day=pd.DataFrame(index=[date],columns=factor_date_preprocessed_df.columns)
        model = sm.OLS(factor_date_preprocessed_df.astype(float), stock_return_shift_se.astype(float)).fit()
        factor_return_one_day = model.params
        return factor_return_one_day
        
#%%
def get_IC_FactorReturn(Factor_date_dict,stock_return_shift_dict):
    '''
    Factor_date_dict:字典：key：date，value：每天的factor是一个df
    stock_return_shift_dict： 字典
                              key：date
                              value：已经滞后过了的stock return！！
                              比如预测为T+2，
                              此时date对应的stock return真实的是date+2天的return
    
    '''
    date_list = list(Factor_date_dict.keys())
    IC_df = pd.DataFrame()
    FactorReturn_df = pd.DataFrame()
    for date in date_list:
        factor_date_df = Factor_date_dict[date]
        stock_return_shift_se = stock_return_shift_dict[date]
        IC_df.append(FactorTestOneDay.get_IC_one_day(factor_date_df,stock_return_shift_se,date))
        FactorReturn_df.append(FactorTestOneDay.get_factor_return_one_day(factor_date_df,stock_return_shift_se,date))
        
    return IC_df,FactorReturn_df
#%%
class MultiFactorTest(IC_df,FactorReturn_df):
    
    def __int__(self):
        self.IC_df = IC_df
        self.FactorReturn_df = FactorReturn_df
        
    def get_ICIR(self):
        return self.IC_df.apply(lambda se: se.mean()/se.std())

    def factor_return_test(self):
        '''
        对于factor return进行
        输入：index是datetime，columns是factor name，
             values是factor return的dateframe
        t检验：是否显著不等于0，输出t统计量与p值
        平稳性检验：利用ADF（单位根检验），输出对应的p值（小于0.05则认为没有单位根，是平稳的）
        '''
        FactorReturn_df=self.FactorReturn_df
        test_res_df = pd.DataFrame(index=["t_value", "p_value","ADF_value"],columns=FactorReturn_df.columns)
        for factor_code in test_res_df.columns:
            sample = FactorReturn_df[factor_code].values
            ttest = stats.ttest_1samp(sample, 0, axis=0)
            ADF = sm.tsa.stattools.adfuller(sample)
            test_res_df[factor_code] = [ttest.__getattribute__("statistic"),ttest.__getattribute__("pvalue"),ADF[1]]
        return test_res_df

#%%









#%%
factor_date_df.apply(np.corrcoef(stock_return_shift_se),axis=0)

np.corrcoef(factor_date_df.iloc[:,1],stock_return_shift_se)




































class CalculateIndicators():
    
    def __init__(self,factor_exposure,stock_return):
        self.factor_exposure = factor_exposure
        self.stock_return = stock_return
        
    def get_IC_oneday(self):
        return self.factor_exposure.apply(lambda se: se.corr(self.stock_return))

    
#%%%
def get_data_shiftT(T):
    return factor_exposure,stock_return_shift

def preprocesing():
    pass