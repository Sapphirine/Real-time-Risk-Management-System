'''
    Copyright [2014]
    [Iljoon Hwang, ih138@columbia.edu
    Sung Joon Huh, sh3246@columbia.edu]
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
   limitations under the License.
'''

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

def prod(x):
    return x[0]* x[1]

def mc(x):
    res_ls=[]
    for i in range(1000):
       res_ls.append( x[0] + x[1]*rd.gauss(0,1) )
    return res_ls

def baseRoutine1(ticks, sc):

    # 1. reading file again to Close dictionary containig RDD of close price
    Close_FM_rdd_dict={}# dict RDD: storing all 5 min close price of someTick as in float 
    Close_daily_rdd_dict={} # dict RDD: storing all close 1 min price of tickers as in float
    Close_rdd_dict={} # combined dict RDD: storing all close price of someTick as in float 

    for t in ticks:
        fileLoc = "hdfs://master:8020/user/hwang/data/" + t
    # create rdd which contains list of prices in float type
        Close_FM_rdd_dict[t] = sc.textFile(fileLoc).map(lambda x: float(x))
    # end of for

    # Collect daily data, slice it to 5 min data then combine it to Close_rdd_dict[t] RDD.
    for t in ticks:
        fileLoc = "hdfs://master:8020/user/hwang/dailyData/" + t
        Close_daily_rdd_dict[t] = sc.textFile(fileLoc).map(lambda x: float(x))
        Close_daily_rdd_dict[t] = sc.parallelize(Close_daily_rdd_dict[t].collect()[::5])
        Close_rdd_dict[t] = Close_FM_rdd_dict[t] + Close_daily_rdd_dict[t]
    # end of for

    # 2. create list of return per tick
    ret_rdd_dict={}
    for t in ticks:
        price_arr = np.array(Close_rdd_dict[t].collect())
        temp = (price_arr[1:] - price_arr[:-1])/price_arr[:-1]
	ret_rdd_dict[t] = sc.parallelize(temp.tolist())
    # end of for

    # 3. create mu sigma tuples in list
    mu_sigma_ls =[]

    for t in ticks:
	mu_sigma_ls.append( (t, [ret_rdd_dict[t].mean(), ret_rdd_dict[t].stdev()] ) )
    # end of for
	
    # 4. monte carlo
    ret_mc_rdd = sc.parallelize(mu_sigma_ls).values().map(mc)
    ret_mc_ls = ret_mc_rdd.collect()

    # 5. demo positions and exposures
    exposures_ls=[]
    positions_ls = (np.ones(len(ticks))*100).tolist()
    i=0
    for t in ticks:
        try:
            exposures_ls.append( positions_ls[i] *float(Close_rdd_dict[t].collect()[-1:][0]) )
        except:
            pass
        i +=1
    # end of for

    # 6. finding each asset values and portfolio values from monte carlo
    i =0
    MC_port=[]
    result_mc_dict={}
    for t in ticks:
        temp_ex_rdd = sc.parallelize( [exposures_ls[i]] )
        temp_mc_rdd = sc.parallelize(ret_mc_ls[i])
        result_mc_dict[t] = temp_ex_rdd.cartesian(temp_mc_rdd).collect()
        MC_port.append(np.sum(result_mc_dict[t]))
        i +=1
    
    for t in ticks:
        result_mc_dict[t] = sc.parallelize(result_mc_dict[t]).map(prod).collect()

    result_mc_df = pd.DataFrame(result_mc_dict, columns=ticks)

    temp = result_mc_df.corr()
    corrM = np.matrix(temp)
    all_sorted_MC_df = pd.DataFrame(pd.concat([result_mc_df[col].order().reset_index(drop=True) for col in result_mc_df], axis=1, ignore_index=True))
    all_sorted_MC_df.columns = ticks

    temp_ls = []
    for t in ticks:
        temp_ls.append(-all_sorted_MC_df[t].values[10000/20])

    temp_var = np.matrix(np.transpose(np.array(temp_ls)))
    VaR_each = pd.DataFrame(temp_var, index=range(1), columns=ticks)
    temp_ls = np.array(temp_ls)
    MC_mat = np.matrix(temp_ls)

    VaR95 = np.sqrt(MC_mat*corrM*np.transpose(MC_mat)) 
    VaR95 = VaR95.tolist()
    VaR95 = VaR95[0]

    return VaR_each, VaR95

# main routine

appName ='GetVar'
conf = SparkConf().setAppName(appName).setMaster('spark://master:7077').set("spark.executor.memory", "1g")
sc = SparkContext(conf=conf)
sym = tick_list("./sp500")
sym100 = sym[-100:]

start_time = time.time()
[VaR_each, VaR, Close_rdd_dict] = baseRoutine1(sym, sc)
print VaR_each
print VaR
end_time= time.time()
print('Duration: {}'.format(end_time - start_time))

