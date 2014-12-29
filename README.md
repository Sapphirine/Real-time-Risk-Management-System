Real-time-Risk-Management-System
================================

Team member: Iljoon Hwang (ih138), Sung Joon Huh (sh3246),  Sung Woo Yoo (sy2513)

Project Website: https://chrisijhwang.wordpress.com/2014/12/23/big-data-analytics-project-from-columbia-university-class/

Introduction
------------
This is the project of Big Data Analytics Course at Columbia University.
This project consists of 2 different jobs to implement of risk management by finding **_Value-At-Risk(VaR)_** using 5-minute-tick-data with [Apache Spark][1]. One is data collecting job which is coded in _Java_. The other is compuation job for finding VaR coded in _Python_ with [**Spark Python API**][2]. This project will give an example of implementing VaR by utlizing not only [**Hadoop ditributed file system**][4] and spark cluster computing platform but also its computing algorithm with iPython's interactive analytic functionality. We used [*Google Cloud Service*][3].

[1]: http://spark.apache.org
[2]: http://spark.apache.org/docs/1.0.2/api/python/index.html
[3]: https://cloud.google.com
[4]: https://hadoop.apache.org


System
------
- Google Cloud Service: Standalone deploy mode. 
  - Master node and workers for Apache Spark.
  - Name/Secondary node, Data nodes
- iPython : For interactive analytics
  - configured with Pyspark module
  - configured for remote access to master server
- Apache Hadoop 1.2.1
  - Name Node, Secondary Name Node, Data Nodes
- Apache Spark 1.0.2
  - Master/Worker, Workers
- Directory Structure of **Hadoop File System**
```  
|-- bdproject/
      |-- data/ : Storing 10-day-5min-Tick-data
      |-- dailyData/ : Storing 1-day-1min-Tick-data
      |-- sp500 : file containing all tickers
```


Software Packages
-----------------

- Files at Master Node
```  
|-- bdproject/
      |-- TickDataReadWrite.java
      |-- ReadTickData.jar
      |-- GetVar.py
      |-- GetVar_least_spark.py
      |-- sp500
```
  
- Data Collecting Module
  - TickDataReadWrite.java : Jave file for data collecting and write them to Apache Hadoop Ditributed File System.
  - ReadTickData.jar : Jar package from TickDataReadWrite.java
  - Using crontab or other utility for automated collecting.
- Computing Module
  - GetVar.py : Python file for computing Value-At-Risk of Portfolio using Apache Spark Python API.
  - GetVar_least_spark.py : Python file for comparing the performance of plain python and the Apache Spark. It uses Apache Spark Python API only for accessing hadoop files. It didn't use any other Apache Spark Python API for computing VaR.
- sample file
  - sp500: Ticker list

Usages
------

1. Replace the Hadoop File System paths in the TickDataReadWrite.java accordingly based on your particular directory structure. However, Yahoo Finance url should not change.
2. Create Jar file then run it. Use Cron or other time based job scheduler for automatic data collecting. 10-day data will be collected once a day before market opens. 1-min data will be collected every 5 minutes while market opens 
  - example:
  
    For 10-day 5-min data

    `hadoop jar ReadTickData.jar com.cijhwang.hadoop.TickDataReadWrite 0`
    
    For 1-day 1-min data
    
    `hadoop jar ReadTickData.jar com.cijhwang.hadoop.TickDataReadWrite 1`
3. Start the ipython with pyspark loaded
4. Connect to ipython server and run GetVar.py
  - example:
  
    `% run GetVar.py`

