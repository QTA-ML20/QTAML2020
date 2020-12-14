# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 20:25:03 2020

@author: Mengjie Ye
"""
import os
import sys
# dbr：从数据库中读取相关数据：主要是指数权重，沪深股票的日行情，已经算好的因子
from data.DBReader import DatabaseReader as dbr
# 数据预处理的三大类：去极值、中性化、标准化
from FactorAnalyse.DataPreProcessing import DeExtremeMethod,NeutralizeMethod,StandardizeMethod
import pandas as pd
import numpy as np
import statsmodels.api as sm
from scipy import stats
import time
#%%
class PrepareDataDict():

    @classmethod
    def get_factor_date_dict(cls, factor_list, start_date, end_date,index_name='000300.SH'):
        ''' 获得因子的字典
        
        从database读取一段时间的某些股票的factor，并将其储存至dict中
        dict的key是date，value是因子值的dataframe（stock x factor）
        根据换仓频率：日度的话，factor每天取一次
        
        :param factor_list： 感兴趣的factor的名称
        :param start_date: 输入指定的开始日期
        :param end_date:  输入指定的结束日期
        :param index_code: 股票指数代码，目前只支持000300.SH 和 000905.SH
        :return factor_date_dict：以date为key，因子值的dataframe（stock x factor）为value的字典
        '''
        
        trade_day = list(dbr.get_all_trade_days(start_date,end_date))
        factor_date_dict = {}
        time_start = time.time()
        for date in trade_day:
            weight_index = dbr.get_index_weight(index_name,date,date).dropna()
            code_list = list(set(weight_index.code))
            code_list.sort()
            daily_factor = dbr.get_daily_factor(code_list, factor_list,date, date)
            tt = daily_factor[daily_factor.datetime == date]
            factor_date_dict[date] = tt.set_axis(tt.code.tolist(),axis=0).drop(columns = ['datetime','code']).loc[code_list].dropna()
            # print(date)
        time_end = time.time()
        print('daily_factor time cost',time_end-time_start,'s')
        return factor_date_dict
    
    @classmethod
    def get_stock_return_shift_dict(cls,start_date, end_date,index_name='000300.SH',shift_days=1):
        ''' 获得shift过后的股票收益字典
        从database读取一段时间的某些股票的Close，计算return
        根据shift_days得到T+n的return，储存到dict中
        dict的key是已经shift了的date，value是股票收益（stock_return）的dataframe
        例如：shift_days为2，则stock_return_shift_dict[date]存储的实际上是！！date+2！！的return
        
        :param factor_list： 感兴趣的factor的名称
        :param start_date: 输入指定的开始日期
        :param end_date:  输入指定的结束日期
        :param index_code: 股票指数代码，目前只支持000300.SH 和 000905.SH
        :param shift_days：要shift的天数，默认为1天，也就是利用 当天以及当天之前的信息 计算出的factor预测下一天的stock_return
        :return stock_return_shift_dict: key为日期，value为该日期对应的已经shift后的那天的stock_return的df
        
        '''
        time_start = time.time()
        all_trade_day = dbr.get_all_trade_days()
        new_end_date = all_trade_day[all_trade_day>end_date][shift_days-1]
        new_start_date = all_trade_day[all_trade_day>start_date][shift_days-1]
        trade_day = list(dbr.get_all_trade_days(start_date,end_date))
        new_trade_day = list(dbr.get_all_trade_days(new_start_date,new_end_date))
        stock_return_shift_dict = {}
        for date,new_date in zip(trade_day,new_trade_day):
            weight_index = dbr.get_index_weight(index_name,date,date).dropna()
            code_list = list(set(weight_index.code))
            code_list.sort()
            daily_quote = dbr.get_daily_quote(code_list, date, date)
            new_daily_quote = dbr.get_daily_quote(code_list, new_date, new_date)
            tt = daily_quote[daily_quote.datetime == date][['close','code']]
            new_tt = new_daily_quote[new_daily_quote.datetime == new_date][['close','code']]
            close = tt.set_axis(tt.code.tolist(),axis=0).drop(columns = ['code']).loc[code_list].dropna()
            new_close = new_tt.set_axis(new_tt.code.tolist(),axis=0).drop(columns = ['code']).loc[code_list].dropna()
            stock_return_shift_dict[date] = (new_close/close - 1).dropna().set_axis(['return'],axis=1)
        time_end = time.time()
        print('stock_return time cost',time_end-time_start,'s')
        return stock_return_shift_dict
    
#%%
class PreprocessFactor():
    '''
    输入未处理的factor_date_dict，设置参数从而选择处理方式
    默认不处理，处理顺序：去极值，中性化，标准化，每一步都可以设置不处理
    想要得到最后的结果就直接 .standardize()
    
    '''
    
    def __init__(self,factor_date_dict,
                 deextreme = None,standard = None, 
                 control_factor_date_dict = None):
        self.factor_date_dict = factor_date_dict
        self.deextreme = deextreme
        self.standard = standard
        self.control_factor_date_dict = control_factor_date_dict
        
    def deExtreme(self):
        factor_date_dict = self.factor_date_dict.copy()
        if self.deextreme is not None:
            deExtremeMethodDict = {
                'mean_std': DeExtremeMethod.Method_Mean_Std,
                'median': DeExtremeMethod.Method_Median,
                'quatile': DeExtremeMethod.Method_Quantile
                }
            
            deExtremeMethod = deExtremeMethodDict[self.deextreme]
            for date in list(factor_date_dict.keys()):
                 factor_date = factor_date_dict[date]
                 factor_date.apply(deExtremeMethod,axis=0)
        return factor_date_dict
    
    def neutralize(self):
        factor_date_dict = self.deExtreme()
        control_factor_date_dict = self.control_factor_date_dict
        if control_factor_date_dict:
            for date in list(factor_date_dict.keys()):
                factor_date = factor_date_dict[date]
                control_factor_date = control_factor_date_dict[date]
                factor_date_dict[date] = factor_date.apply(NeutralizeMethod.Method_Residual,control_factor_df=control_factor_date,axis=0)
        return factor_date_dict
        
    
    def standardize(self):
        factor_date_dict = self.neutralize()
        if self.standard is not None:
            standardMethodDict = {
                'zscore': StandardizeMethod.Method_Z_Score,
                '0-1': StandardizeMethod.Method_0_1,
                '1-1': StandardizeMethod.Method_1_1,
                'percentile': StandardizeMethod.Method_Percentile
                }
            standardMethod = standardMethodDict[self.standard]
            for date in list(factor_date_dict.keys()):
                factor_date = factor_date_dict[date]
                factor_date.apply(standardMethod,axis=0)
        return factor_date_dict
#%%   
    
class FactorIndicatorOneDay():
    '''
    计算每天的IC或者factor_return
    
    :param factor_date: df 某一天的factor
    :param stock_return_shift：某一天的要预测的stock_return
    
    :returns ic_one_day: 这天对应的IC
    :returns factor_return_one_day: 这天对应的factor return
    ！！！ 这里的return都是用到该日期的未来信息做回归/corr计算出来的
    '''
    def __init__(self,factor_date,stock_return_shift,date='date'):
        self.factor_date = factor_date
        self.stock_return_shift = stock_return_shift
        self.date = date
        # self.factor_return_df = factor_return_df
        
    def get_IC1d(self):
        factor_date = self.factor_date
        date = self.date
        stock_return_shift = self.stock_return_shift
        
        ic_one_day=pd.DataFrame(index=[date],columns=factor_date.columns)
        for i in range(factor_date.shape[1]):
            data_df = pd.concat([factor_date.iloc[:,i],stock_return_shift],join = 'inner',axis=1).dropna()
            ic_one_day.iloc[0,i]=data_df.corr().iloc[0,1]
        return ic_one_day
    
    def get_factor_return1d(self):
        factor_date = self.factor_date
        date = self.date
        stock_return_shift = self.stock_return_shift
        data_df = pd.concat([factor_date,stock_return_shift],join='inner',axis=1).dropna()
        # factor_return_one_day=pd.DataFrame(index=[date],columns=factor_date.columns)
        model = sm.OLS(data_df.iloc[:,:-1].astype(float), data_df.iloc[:,-1].astype(float)).fit()
        factor_return_one_day=pd.DataFrame(model.params.values.reshape(1,-1),index=[date],columns=factor_date.columns)
        return factor_return_one_day
    
#%%   
class FactorTest():
    '''
    输入factor_date_dict与stock_return_shift_dict
    计算IC_df与factor_return_df并进行检验
    （计算ICIR以及factor_return的t-检验）
    也可以直接输入IC_df或者factor_return_df进行检验
    
    根据factor_return_df与fr_win计算出factor_return_pred_dict
    '''
    def __init__(self,factor_date_dict = None,stock_return_shift_dict = None,
                 factor_return_df = None,IC_df = None,fr_win = 20):
        self.factor_date_dict = factor_date_dict
        self.stock_return_shift_dict = stock_return_shift_dict
        self.factor_return_df = factor_return_df
        self.IC_df = IC_df
        self.fr_win = fr_win
        
    def get_IC_factorReturn_df(self):
        factor_date_dict = self.factor_date_dict
        stock_return_shift_dict = self.stock_return_shift_dict
        IC_df = pd.DataFrame()
        factor_return_df = pd.DataFrame()
        for date in list(factor_date_dict.keys()):
            factor_date = factor_date_dict[date]
            stock_return_shift = stock_return_shift_dict[date]
            aa = FactorIndicatorOneDay(factor_date,stock_return_shift,date)
            IC_df = IC_df.append(aa.get_IC1d())
            factor_return_df = factor_return_df.append(aa.get_factor_return1d())
        self.IC_df, self.factor_return_df = IC_df,factor_return_df
        return IC_df, factor_return_df
    
    def get_ICIR(self):
        if self.IC_df is not None:
            IC_df = self.IC_df
        else:
            IC_df,_ = self.get_IC_factorReturn_df()
        return pd.DataFrame(IC_df.mean()/IC_df.std()).T.set_axis(['ICIR'],axis=0)
            
    def factor_return_test(self):
        if self.factor_return_df is not None:
            factor_return_df = self.factor_return_df.copy(deep=True)
        else:
            _,factor_return_df = self.get_IC_factorReturn_df()
            
        test_res_df = pd.DataFrame(index=["t_value", "p_value","ADF_value","positive_ratio"],columns=factor_return_df.columns)
        for factor_code in test_res_df.columns:
            sample = factor_return_df[factor_code].values
            ttest = stats.ttest_1samp(sample, 0, axis=0)
            ADF = sm.tsa.stattools.adfuller(sample)
            positive_ratio = (sample>0).sum()/len(sample)
            test_res_df[factor_code] = [ttest.__getattribute__("statistic"),ttest.__getattribute__("pvalue"),ADF[1],positive_ratio]
        return test_res_df
    
    def get_factor_return_pred_dict(self,shift_days=1):
        fr_win = self.fr_win
        if self.factor_return_df is not None:
            factor_return_df = self.factor_return_df
        else:
            IC_df,factor_return_df = self.get_IC_factorReturn_df()
            self.IC_df,self.factor_return_df = IC_df,factor_return_df
        factor_return_pred_df = factor_return_df.shift(shift_days).fillna(method='bfill').rolling(fr_win,min_periods=1).mean()
        factor_return_pred_dict = {}
        for date in list(factor_return_pred_df.index):
            factor_return_pred_dict[date] = factor_return_pred_df.loc[date]
        return factor_return_pred_dict
        
#%%

def get_nev_groupbt(factor_preprocessed_date_dict,
                    factor_return_pred_dict,
                    stock_return_shift_dict):
    '''
    分组回测
    :param: factor_preprocessed_date_dict: 以date为key，value是factor_df的dict,已经经过了预处理
    :param: factor_return_pred_dict：以date为key，value是factor_return_pred的dict，已经shift了
    :param: stock_return_shift_dict：以date为key，value是stock_return_shift的dict，已经shift了
    :return: 策略的净值((1+return).cumprod())
    '''
    retadd1_df = pd.DataFrame(None,index = list(factor_preprocessed_date_dict.keys()),columns=['NetValue'])
    for date in list(factor_preprocessed_date_dict.keys()):
        # 取当天的factor
        factor_date = factor_preprocessed_date_dict[date]
        # 取下一期的factor return 的预测值（需要shift）
        factor_return_pred_date = factor_return_pred_dict[date]
        # 取下一期的stock_return 的真实值（shift后）series
        stock_return_shift_date = stock_return_shift_dict[date]
        # 计算每个stock_return 的预测值，然后排序取前20%
        stock_return_pred_date = factor_date*factor_return_pred_date
        stock_backtest_index = stock_return_pred_date.sum(axis=1).sort_values(ascending=False).head(int(len(stock_return_pred_date)*0.2)).index
        
        return_backtest = stock_return_shift_date.loc[stock_backtest_index].mean()
        
        retadd1_df.loc[date] = float(return_backtest+1)
    nev_df = retadd1_df.cumprod()
    return nev_df,retadd1_df


#%%
def get_backtest_indicator(nev_df,nums_tradeday_oneyear = 250):
    # NetValue 可视化，输出相关指标
    
    return_df = (nev_df/nev_df.shift().fillna(1) - 1).set_axis(['return'],axis=1)
    # 年化收益率
    annual_return = (float(nev_df.iloc[-1])**(nums_tradeday_oneyear))**(1/len(nev_df)) - 1
    # 年化波动率
    annual_volatility = float(return_df.std()) * np.sqrt(nums_tradeday_oneyear)
    # 夏普比率
    sharp_ratio = annual_return/annual_volatility 
    # 最大回撤
    nev_max_df = nev_df.rolling(len(nev_df),min_periods=1).max()
    nev_minus_ratio_df = (nev_max_df - nev_df)/nev_df
    drawdown_ratio_df = 100 * nev_minus_ratio_df.rolling(len(nev_minus_ratio_df),min_periods=1).max()
    
    res_df = pd.DataFrame(data=[annual_return,annual_volatility,sharp_ratio,float(drawdown_ratio_df.iloc[-1])],
                          index=['annual_return','annual_volatility','sharp_ratio','drawdown(ratio)%'],
                          columns=['Backtest Result'])
    
    return res_df



#%%
if __name__ == "__main__":
    # 设置基本信息
    start_date='20170801'
    end_date='20200801'
    factor_list = ['turnover_ratio','circulating_market_cap','pe_ratio','pb_ratio']
    # 获取这段时间每天的factor，存入字典
    factor_date_dict = PrepareDataDict.get_factor_date_dict(factor_list,start_date,end_date)
    # 获取研究时间段的需要预测的stock_return，存入字典
    stock_return_shift_dict = PrepareDataDict.get_stock_return_shift_dict(start_date, end_date,index_name='000300.SH')
    # 对于factor_date_dict中的因子进行预处理，这里只进行mean_std的极值处理与zscore的标准化处理，
    # 并没有进行中性化，想要中性化，可以加入control_factor_dict
    preprocess = PreprocessFactor(factor_date_dict,deextreme='mean_std',standard='zscore')
    factor_preprocessed_date_dict = preprocess.standardize()
    # 计算IC与factor_return,并进行检验
    factorTest = FactorTest(factor_preprocessed_date_dict,stock_return_shift_dict)
    IC_df,factor_return_df = factorTest.get_IC_factorReturn_df()    
    ICIR = factorTest.get_ICIR()
    factor_return_test = factorTest.factor_return_test()
    # 获取每天factor_return的预测值（用shift_days天之前的factor_return通过MA的形式）
    factor_return_pred_dict = factorTest.get_factor_return_pred_dict()
    # 分组回测，并计算策略的净值
    nev_df,retadd1_df = get_nev_groupbt(factor_preprocessed_date_dict,
                             factor_return_pred_dict,
                             stock_return_shift_dict)
    # 计算策略的相关评价指标
    res_df = get_backtest_indicator(nev_df)
    # 将净值曲线画出
    nev_df.plot()
    # get code_list
    # Weight_index = dbr.get_index_weight('000300.SH',start_date,end_date).dropna()
    # Weight_index = Weight_index.pivot(index = 'datetime',columns = 'code',values='weight')
    # code_list = list(Weight_index.columns)
    # trade_day = list(dbr.get_all_trade_days(start_date,end_date))
    # daily quote
    # daily_quote = dbr.get_daily_quote(code_list, start_date, end_date)
    # close = daily_quote.pivot(index = 'datetime',columns = 'code',values = 'close')
    # raw_close = daily_quote.pivot(index = 'datetime',columns = 'code',values = 'raw_close')
    # 例如：直接让close作为当天的factor
    
    # daily_factor = dbr.get_daily_factor(code_list, factor_list, start_date, end_date)

    
    # trade_day = list(dbr.get_all_trade_days(start_date,end_date))
    # factor_date_dict = {}
    # for date in trade_day:
    #     tt = daily_factor[daily_factor.datetime == date]
    #     factor_date_dict[date] = tt.set_axis(tt.code.tolist(),axis=0).drop(columns = ['datetime','code']).loc[code_list]
    # factor_date = ffp.copy()
    # ic_one_day=pd.DataFrame(index=[date],columns=factor_date.columns)
    # stock_return_shift = stock_return_shift_dict[date]
    # for i in range(factor_date.shape[1]):
    #     ic_one_day.iloc[0,i]=pd.concat([factor_date.iloc[:,i],stock_return_shift],axis=1).dropna().corr().iloc[0,1]


 
