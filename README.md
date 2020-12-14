# QTAML2020

[工作时间统计](https://docs.qq.com/sheet/DUmF3Z0ZrTE9lTWla)  
 [会议记录](https://github.com/QTA-ML20/QTAML2020/blob/main/meeting_log)

## 项目介绍

本次project主要包括以下几点：

- 自主搭建数据库，写好接口文件，实现数据的读取、修改与存储
- 搭建多因子模型框架，实现单因子构建与检验、多因子合成、模型测试、回测结果分析等

## 数据库

## MultiFactorModel

MultiFactorModel主要实现以下功能：

- PrepareDataDict：从database读取一段时间的数据，并将其储存至dict，dict的key是date，value是相关数据的dataframe（stock_code x data_col_name）
- PreprocessFactor：对于factor进行预处理操作，包括极值处理、中性化与标准化，用户可根据自身需求选择处理的方式
- FactorTest：对于factor进行测试，包括一段时间IC、ICIR、factor return的分布情况
- get_nev_groupbt：多因子回测，得到策略净值曲线
- get_backtest_indicator：对于回测结果进行展示