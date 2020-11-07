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
# =============================================================================
# 下面的路径是用来import叶文轩写的DataPreProcessing，可以换自己的路径
# 也可以直接把工作路径改到"D:\QTAML2020\FactorAnalyse"
# =============================================================================
# path_wx='d://QTAML2020//FactorAnalyse'
# path_preprocessing ='d://QTA/project//code//PreProcessing'
# sys.path.append(path_wx)
# sys.path.append(path_preprocessing)
# import FactorAnalyse
from DataPreProcessing import DeExtremeMethod,NeutralizeMethod,StandardizeMethod
# from data.DBReader import DatabaseReader
# from PreProcessing import get_daily_factor_preprocessed

#%%
class FactorTestOneDay():
    '''
    输入
    ---
    factor_date：dataframe；某一天（date）的factor
    stock_return_shift_se：series；滞后T期的stock_return
    date：datetime；factor对应的日期
    control_factor_date：dataframe；中性化时所需要的控制变量，若为None，则不进行中性化
    
    功能
    ---
    preprocessing_one_day：对factor_date进行预处理
    get_IC_one_day：根据factor_date和stock_return_shift_se求该天的IC
    get_factor_return_one_day：根据factor_date和stock_return_shift_se求该天的
    
    '''
    def __init__(self,factor_date,stock_return_shift_se,date,control_factor_date=None):
        self.factor_date = factor_date
        self.stock_return_shift_se = stock_return_shift_se
        self.date = date
        self.control_factor_date = control_factor_date
        
    def preprocessing_one_day(self):
        factor_date = self.factor_date
        control_factor_date = self.control_factor_date
        #去极值
        ff = factor_date.apply(DeExtremeMethod.Method_Mean_Std,axis=0)
        #中性化
        if control_factor_date:
            ff_neu = ff.apply(NeutralizeMethod.Method_Residual,control_factor_df=control_factor_date,axis=0)
        else:
            ff_neu = ff
        #Zscore
        factor_date_preprocessed = ff_neu.apply(StandardizeMethod.Method_Z_Score)
        
        return factor_date_preprocessed
    
    def get_IC_one_day(self):
        factor_date_preprocessed = self.preprocessing_one_day()
        stock_return_shift_se = self.stock_return_shift_se
        date = self.date
        ic_one_day=pd.DataFrame(index=[date],columns=factor_date_preprocessed.columns)
        for i in range(factor_date_preprocessed.shape[1]):
            ic_one_day.iloc[0,i]=np.corrcoef(factor_date_preprocessed.iloc[:,i],stock_return_shift_se)[0,1]
        return ic_one_day
    
    def get_factor_return_one_day(self):
        factor_date_preprocessed = self.preprocessing_one_day()
        stock_return_shift_se = self.stock_return_shift_se
        date = self.date
        factor_return_one_day=pd.DataFrame(index=[date],columns=factor_date_preprocessed.columns)
        model = sm.OLS(factor_date_preprocessed.astype(float), stock_return_shift_se.astype(float)).fit()
        factor_return_one_day = model.params
        return factor_return_one_day
        
#%%
def get_IC_FactorReturn(factor_date_dict,stock_return_shift_dict):
    '''
    输入
    ---
    factor_date_dict:字典；
                    key：date；
                    value：每天的factor是一个df
    stock_return_shift_dict： 字典
                              key：date
                              value：已经滞后过了的stock return！！
                              比如预测为T+2，
                              此时date对应的stock return真实的是date+2天的return
    输出
    ---
    这段时间里的IC和factor return
    '''
    date_list = list(factor_date_dict.keys())
    IC_df = pd.DataFrame()
    factor_return_df = pd.DataFrame()
    for date in date_list:
        factor_date = factor_date_dict[date]
        stock_return_shift_se = stock_return_shift_dict[date]
        aa = FactorTestOneDay(factor_date,stock_return_shift_se,date)
        IC_df = IC_df.append(aa.get_IC_one_day())
        factor_return_df = factor_return_df.append(aa.get_factor_return_one_day())
        
    return IC_df,factor_return_df
#%%
class MultiFactorTest():
    
    def __init__(self,IC_df,factor_return_df):
        self.IC_df = IC_df
        self.factor_return_df = factor_return_df
        
    def get_ICIR(self):
        return (pd.DataFrame(self.IC_df.apply(lambda se: se.mean()/se.std())).T).set_axis(['ICIR'],axis=0)

    def factor_return_test(self):
        '''
        输入
        ---
        factor_return_df:dataframe;
                        index是datetime，columns是factor name，values是factor return
        功能
        ---
        t检验：factor_return是否显著不为0，输出对应t值与p值
        平稳性检验：利用ADF（单位根检验），输出对应的p值，小于0.05则认为没有单位根，是平稳的
        '''
        factor_return_df=self.factor_return_df
        test_res_df = pd.DataFrame(index=["t_value", "p_value","ADF_value"],columns=factor_return_df.columns)
        for factor_code in test_res_df.columns:
            sample = factor_return_df[factor_code].values
            ttest = stats.ttest_1samp(sample, 0, axis=0)
            ADF = sm.tsa.stattools.adfuller(sample)
            test_res_df[factor_code] = [ttest.__getattribute__("statistic"),ttest.__getattribute__("pvalue"),ADF[1]]
        return test_res_df



#%%

# 假设已经有了算好的因子字典
# key是date，value是factor_date
if __name__ == "__main__":
    
    
    n = 5
    n_stock = 300
    # date='20200101'
    factor_date_dict = {'20200101':pd.DataFrame(np.random.random((n_stock,n))),
                        '20200102':pd.DataFrame(np.random.random((n_stock,n))),
                        '20200103':pd.DataFrame(np.random.random((n_stock,n))),
                        '20200104':pd.DataFrame(np.random.random((n_stock,n)))}
    # factor_date = factor_date_dict[date]
    stock_return_shift_dict = {'20200101':pd.Series(np.random.randn((n_stock))),
                               '20200102':pd.Series(np.random.randn((n_stock))),
                               '20200103':pd.Series(np.random.randn((n_stock))),
                               '20200104':pd.Series(np.random.randn((n_stock)))}
    # stock_return_shift_se = stock_return_shift_dict[date]
    IC_df,factor_return_df = get_IC_FactorReturn(factor_date_dict,stock_return_shift_dict)
    
    multifactortest = MultiFactorTest(IC_df,factor_return_df)
    ICIR = multifactortest.get_ICIR()
    factor_return_test_res = multifactortest.factor_return_test()
    print('ICIR is as follows:\n',ICIR)
    # print(ICIR)
    print('MultiFactor Test Result is as follows:\n',factor_return_test_res)
    # print(factor_return_test_res)
