import numpy as np
from scipy import stats
import pandas as pd
import statsmodels.api as sm
from statsmodels.formula.api import ols
import  datetime

# 输入字典列表算出来每个factor每日的factor_return,得到字典类型的数据
def facreturn_mis(all_factor,begindate,enddate):
    begin_year,begin_month,begin_date = begindate.split('-',3)
    end_year, end_month, end_date = enddate.split('-', 3)
    begin = datetime.date(int(begin_year),int(begin_month),int(begin_date))
    end = datetime.date(int(end_year), int(end_month), int(end_date))
    facreturn_mis_dic = {'factor1': [],'factor2':[],'factor3':[],'mis':[]}
    for i in range((end - begin).days + 1):
        day = begin + datetime.timedelta(days=i)
        daily_factor = all_factor[str(day)]
        # 因子的名称先用factor_1代替
        X = daily_factor[['factor1','factor2','factor3']]
        Y = daily_factor[['ret']]
        X = sm.add_constant(X)
        model = sm.OLS(Y.astype(float), X.astype(float)).fit()

        facreturn_mis_dic['factor1'].append(model.params['factor1'])
        facreturn_mis_dic['factor2'].append(model.params['factor2'])
        facreturn_mis_dic['factor3'].append(model.params['factor3'])
        facreturn_mis_dic['mis'].append(model.params['const'])
    return facreturn_mis_dic

#输入上一个函数得到的字典进行每个factor的t检验和平稳性检验
def factor_test(facreturn_mis_dic):
    df_test = pd.DataFrame(columns=["t_value", "p_value","ADF_value"])
    for key in facreturn_mis_dic.keys():
        sample = np.asarray(facreturn_mis_dic[key])
        ttest = stats.ttest_1samp(sample, 0, axis=0)
        ADF = sm.tsa.stattools.adfuller(sample)
        df_test.loc[key] = {'t_value':ttest.__getattribute__("statistic"),'p_value':ttest.__getattribute__("pvalue"),'ADF_value':ADF[1]}
    return df_test





if __name__=='__main__':
    
    factor = pd.DataFrame(np.random.rand(100,3), columns=('factor1', 'factor2', 'factor3'))
    factor['code'] = [str(x).zfill(6)+'.sz' for x in range(100)]
    factor['ret'] = np.random.rand(len(factor))
    all_factor = {'2020-01-02':factor,'2020-01-03':factor,'2020-01-04':factor,'2020-01-05':factor,'2020-01-06':factor,'2020-01-07':factor,'2020-01-08':factor,'2020-01-09':factor,'2020-01-10':factor,'2020-01-11':factor,'2020-01-12':factor,'2020-01-13':factor,'2020-01-14':factor,'2020-01-15':factor,'2020-01-16':factor,'2020-01-17':factor,'2020-01-18':factor,'2020-01-19':factor,'2020-01-20':factor,'2020-01-21':factor,'2020-01-22':factor,'2020-01-23':factor}
    f_r_1 = facreturn_mis(all_factor,'2020-01-02','2020-01-23')
    print(f_r_1)
    T_test = factor_test(f_r_1)
    print(T_test)
