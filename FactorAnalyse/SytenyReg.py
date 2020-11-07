# -*- coding: utf-8 -*-
"""
Created on Mon Oct 26 19:16:05 2020

@author: fengzhe1302
"""
import pandas as pd
import numpy as np
import statsmodels.formula.api as smf
import sys
sys.path.append('..')
from data.DBReader import DatabaseReader as dbr

class SytenyReg():
    @classmethod
    def regression(cls, df, factor, p_value=0.05, retro=60):  
        '''
        因子共线性回归
        :param: df:包含股票，日期及因子暴露的数据
        :param: factor: 所关心的与其他指标有无共线性的指标
        :params: p_value:检验所用显著性水平，默认值为0.05
        :param: retro:相关性检验所用数据回溯期数，如果没有输入，默认为60
        :return: corr_factors:与所关心指标有共线性的指标, 用户自行选择进行后续操作
        
        '''
        factors = list(df)
        dates = list(set(df['datetime']))
        dates.sort()
        s_date = dates[retro*-1]
        df_curr = df[(df['datetime'] >= s_date)]
        factors.remove('code')
        factors.remove('datetime')
        
        corr_factors = []
        temp = factors.copy()
        temp.remove(factor)
        formula = '+'.join(temp)
        formula = str(factor)+' ~ '+formula
        model = smf.ols(formula = formula, data=df_curr[factors]).fit()
        x = model.pvalues
        nonzero = x[x<p_value]
        if('Intercept' in list(nonzero.index)):
            nonzero = nonzero.drop('Intercept')
        if(len(nonzero)!=0):
            for i in range(len(nonzero)):
                corr_factors.append(nonzero.index[i])
        return(corr_factors)       
    
if __name__ == '__main__':
    start_date = '2020-01-01'
    end_date = '2020-03-31'
    p_value=0.05
    stock_list = list(dbr.get_index_weight('000300.XSHG',start_date, end_date)['code'])
    #假设是这些因子，有数据再改
    factor_list = ['factor'+str(i+1) for i in range (10)]
    df = dbr.get_daily_factor(stock_list,factor_list,start_date,end_date)
    corr_factors = SytenyReg.regression(df,'factor8')
