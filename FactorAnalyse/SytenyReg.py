# -*- coding: utf-8 -*-
"""
Created on Mon Oct 26 19:16:05 2020

@author: fengzhe1302
"""
import pandas as pd
import numpy as np
import statsmodels.formula.api as smf
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

class SytenyReg():
    '''
    因子共线性回归
    '''
    def __init__(self, p_value, retro, df):
        self.retro = retro
        self.df = df
        self.p_value = p_value
        
    def regression(self, p_value, retro, df):  
        factors = list(df)
        dates = list(set(df['datetime']))
        dates.sort()
        s_date = dates[retro*-1]
        df_curr = df[(df['datetime'] >= s_date)]
        factors.remove('code')
        factors.remove('datetime')
        
        corr_factors = []
        for factor in factors:
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
                    corr_factors.append(str(factor)+ '+' +str(nonzero.index[i]))
                
def main():
    Reg = SytenyReg(0.05, 60, df)
    
if __name__ == '__main__':
    main() 
