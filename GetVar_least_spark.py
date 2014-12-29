__author__ = 'hwang'

import pandas as pd
import numpy as np
import random as rd
from pyspark import SparkContext, SparkConf
import time

def tick_list(file):
    symbols = pd.read_csv(file,sep='\t', header=None)
    ls_symbols = symbols[0]
    ls_symbols = ls_symbols.tolist()
    return ls_symbols

def baseRoutine1(ticks, sc):
    # routine for deleting tick which didn't change here

    allTick = ticks  # all tickers requested
    someTick = ticks # after checking, tickers which need to read again

    # 1. reading file again to Close dictionary containig RDD of close price
    Close_FM_rdd_dict={}# dict RDD: storing all 5 min close price of someTick as in float 
    Close_daily_rdd_dict={} # dict RDD: storing all close 1 min price of tickers as in float
    Close_rdd={} # dict RDD: storing all close price of someTick as in unicode
    Close_float_rdd={} # dict RDD: strong all close price of someTick as in float
    Close={} # dict when creating dataframe for input of getVaR()
    ret_float_rdd={} # dict RDD: strong all ret of target someTick as in float
    ret_dict={} # dict : strong all ret of target someTick
    lenOfret_ls=[]
    #for t in ticks:
    #    fileLoc = "hdfs://master:8020/user/hwang/data/" + t
    #    Close_rdd[t] = sc.textFile(fileLoc).cache()

    for t in ticks:
        fileLoc = "hdfs://master:8020/user/hwang/data/" + t
        # create rdd which contains list of prices in float type
        Close_FM_rdd_dict[t] = sc.textFile(fileLoc).cache()

        # Collect daily data, slice it to 5 min data then combine it to Close_rdd_dict[t] RDD.
        fileLoc = "hdfs://master:8020/user/hwang/dailyData/" + t
        Close_daily_rdd_dict[t] = sc.textFile(fileLoc).cache()
        Close_daily_rdd_dict[t] = sc.parallelize(Close_daily_rdd_dict[t].collect()[::5])
        Close_rdd[t] = Close_FM_rdd_dict[t] + Close_daily_rdd_dict[t]

        # 2. create list of price per tick
        price=[] # price
        for x in Close_rdd[t].take(Close_rdd[t].count()):
            price.append(float(x))
        price_arr = np.array(price)
        price_ls = price_arr.tolist()
        ret_arr = (price_arr[1:] - price_arr[:-1])/price_arr[:-1]
        ret_ls = ret_arr.tolist()

        # 3. create new rdd from list above
        Close_float_rdd[t] = sc.parallelize(price_ls)
        #Close[t] = price[-500:]
        Close[t] = price
        ret_float_rdd[t] = sc.parallelize(ret_ls)
        #ret_dict[t] = ret_ls[-500:]
        ret_dict[t] = ret_ls
        lenOfret_ls.append(len(ret_ls))
    # end of for

    # 4. create inputs for getVar function
        # allTick: price, positions_ls, ret_target_df
    n = min(lenOfret_ls)
    ret_dict_n={} # storing same number of returns of each ticker
    Close_n={} # storing same number of price of each ticker
    for t in ticks:
        ret_dict_n[t]= ret_dict[t][-n:]
        Close_n[t] = Close[t][-n:]

    ret_target_df =pd.DataFrame.from_dict(ret_dict_n)
    
    #ret_target_df = pd.DataFrame.from_dict(ret_dict)
    price_df = pd.DataFrame(Close_n)
    positions_ls= (np.ones(len(ticks))*100).tolist()
    [a, b] = getVaR(ticks, price_df, positions_ls, ret_target_df, 10000)

    return [a, b]


def getVaR(ticker, price, positions_ls, ret_target_df, no_simul):
    i=0
    exposures = []
    for t in (ticker):
        exposures.append(positions_ls[i]*price[t][len(price[t])-1])
        i +=1
    all_MC_df = pd.DataFrame(index=range(no_simul), columns=ticker)
    MC_port = []
    for i in range(no_simul):
        ret_MC = []
        # MC return for each assets
        for t in ticker:
            ret_MC.append(np.mean(ret_target_df[t]) + np.std(ret_target_df[t]) * rd.gauss(0,1))

        # Compute the exposure * return for each asset
        MC_each_ls = []
        for k in range(len(ticker)):
            MC_each_ls.append(exposures[k]*ret_MC[k])

        all_MC_df.loc[i] = MC_each_ls
    #    Sum --> total portfolio value at time i
        MC_port.append(np.sum(all_MC_df.loc[i]))
    temp = ret_target_df.corr()
    corrM = np.matrix(temp)
    all_sorted_MC_df = pd.DataFrame(pd.concat([all_MC_df[col].order().reset_index(drop=True) for col in all_MC_df], axis=1, ignore_index=True))
    all_sorted_MC_df.columns = ticker

    temp_ls = []
    for t in ticker:
        temp_ls.append(-all_sorted_MC_df[t].values[no_simul/20])

    temp_var = np.matrix(np.transpose(np.array(temp_ls)))
    VaR_each = pd.DataFrame(temp_var, index=range(1), columns=ticker)
    temp_ls = np.array(temp_ls)
    MC_mat = np.matrix(temp_ls)

    VaR95 = np.sqrt(MC_mat*corrM*np.transpose(MC_mat))
    VaR95 = VaR95.tolist()
    VaR95 = VaR95[0]

    return VaR_each, VaR95

appName ='Routine5-01'
conf = SparkConf().setAppName(appName).setMaster('spark://master:7077').set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)
sym = tick_list("./sp500")
sym100 = sym[-100:]

start_time = time.time()
#[a, b] = baseRoutine1(['AA', 'AAPL'], sc)
#[VaR, VaR_Total, Close_float_rdd, ret_float_rdd] = baseRoutine1(sym, sc)
[a, b] = baseRoutine1(sym100, sc)
print a
print b
end_time= time.time()
print('Duration: {}'.format(end_time - start_time))

