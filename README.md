# GhcndDataEventSender
This project's purpose is 
- Python script which used to send ghcnd data to mock a **real-time event data source**.
- _ghcnd_ stand for "GLOBAL HISTORICAL CLIMATOLOGY NETWORK DAILY"


### Where to download
- [Where to download](https://www.kaggle.com/noaa/noaa-global-historical-climatology-network-daily)
- Official Documentation For Data _ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt_
- [National Climatic Data Center](https://www.ncdc.noaa.gov)


### Data Topic

GHCN-D is a dataset that contains daily observations over global land areas. 
Like its monthly counterpart, GHCN-Daily is a composite of climate records from 
numerous sources that were merged together and subjected to a common suite of quality 
assurance reviews. The archive includes the following meteorological elements:

* Daily maximum temperature
* Daily minimum temperature
* Temperature at the time of observation
* Precipitation (i.e., rain, melted snow)
* Snowfall
* Snow depth
* Other elements where available

Please check:
- [detail](./metadata/_ghcnd_readme.txt)
- [sample data](sample/sample.csv)

### Send Event Schema

Dimension 维度
- station_id 气象站点ID
- date 观测日期
- year 观测年份
- month 观测月份
- day 观测日
- country 国家简称
- country_name 国家名称
- us_state 美国州简称
- us_state_name 美国州名
- latitude 经度
- longitude 纬度
- elevation 海拔

Measure 度量
- max_temperature TMAX 最高温度
- min_temperature TMIN 最低温度
- avg_temperature TAVG 平均温度
- precipitation PRCP 降水量（毫米）
- snow_depth SNWD 积雪深度（毫米）
- snow_fall SNOW 当日降雪深度（毫米）
- WESD Water equivalent of snow on the ground (tenths of mm)
- AWND wind speed 风速
- WSF2 Fastest 2-minute wind speed (tenths of meters per second)


### Data Size
- 14.99 GB csv file in gzip.
- About 3 billion raw data.

### Data Prepare

### Script Usage

* Following script to create table into Hive
```sh
[root@cdh8 GhcndDataEventSender]# sh hive-data.sh
+ sh -x setenv.sh
+ export 'raw_file=/root/xiaoxiang/GhcndDataEventSender/sample/*.gz'
+ raw_file='/root/xiaoxiang/GhcndDataEventSender/sample/*.gz'
+ echo 'RawFile at /root/xiaoxiang/GhcndDataEventSender/sample/*.gz'
RawFile at /root/xiaoxiang/GhcndDataEventSender/sample/*.gz
+ echo $'\346\270\205\351\231\244\344\270\212\346\254\241\347\224\237\346\210\220\347\232\204\346\225\260\346\215\256\346\226\207\344\273\266'
清除上次生成的数据文件
+ rm -f 'fact_*'
+ cd source
+ echo $'\347\224\237\346\210\220\347\273\264\345\272\246\350\241\250\346\225\260\346\215\256'
生成维度表数据
+ python metadata_reader.py
+ echo $'\347\224\237\346\210\220\344\272\213\345\256\236\350\241\250\346\225\260\346\215\256'
生成事实表数据
+ python data_sender.py --data-path '/root/xiaoxiang/GhcndDataEventSender/sample/*.gz' --sleep-millsecond-per-thousand 0 --output-format csv
To be read files count 1
[
 "/root/xiaoxiang/GhcndDataEventSender/sample/1872.csv.gz"
]
>>>STATS<<< [/root/xiaoxiang/GhcndDataEventSender/sample/1872.csv.gz] : event count 134036, send 75373 msg, start from 2019-03-19 13:42:18.766477 ,duration is 0:00:01.669246, rate 45153.9197937 msg/sec
+ cd ..
+ ls -l fact_1872
-rw-r--r--. 1 root root 5938375 Mar 19 13:42 fact_1872
++ ls fact_1872
+ datafiles=fact_1872
+ echo $'\351\201\215\345\216\206\344\272\213\345\256\236\350\241\250\346\225\260\346\215\256\346\226\207\344\273\266\357\274\214\345\271\266\344\270\224\350\277\233\350\241\214\345\210\207\345\211\262'
遍历事实表数据文件，并且进行切割
+ for single_f in '$datafiles'
+ echo 'split fact_1872'
split fact_1872
+ split -a 4 -d -l 100000 fact_1872 fact_1872-
+ rm -f fact_1872
+ ls -l fact_1872-0000
-rw-r--r--. 1 root root 5938375 Mar 19 13:42 fact_1872-0000
+ echo '创建 上传HDFS文件脚本和增加hive Partition脚本'
创建 上传HDFS文件脚本和增加hive Partition脚本
+ cd source
+ python create_scripts.py '/root/xiaoxiang/GhcndDataEventSender/sample/*.gz'
Args ['create_scripts.py', '/root/xiaoxiang/GhcndDataEventSender/sample/*.gz']
Search /root/xiaoxiang/GhcndDataEventSender/sample/1872.csv.gz
+ cd ../ddl
+ echo '上传 数据文件'
上传 数据文件
+ sh -x upload_hdfs.sh
+ hadoop fs -rm -r /LacusDir/data/hive/ghcn/
19/03/19 13:42:22 INFO fs.TrashPolicyDefault: Moved: 'hdfs://cdh1.cloudera.com:8020/LacusDir/data/hive/ghcn' to trash at: hdfs://cdh1.cloudera.com:8020/user/root/.Trash/Current/LacusDir/data/hive/ghcn1552974142053
+ hadoop fs -mkdir -p /LacusDir/data/hive/ghcn/1872-01-01/
+ hadoop fs -copyFromLocal '../fact_1872_*' /LacusDir/data/hive/ghcn/1872-01-01/
copyFromLocal: `../fact_1872_*': No such file or directory
+ echo '创建 Hive表'
创建 Hive表
+ hive -f create.ddl
ls: cannot access /root/xiaoxiang/lib/spark-2.3.2//lib/spark-assembly-*.jar: No such file or directory
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0
Java HotSpot(TM) 64-Bit Server VM warning: Using incremental CMS is deprecated and will likely be removed in a future release
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0

Logging initialized using configuration in jar:file:/opt/cloudera/parcels/CDH-5.15.1-1.cdh5.15.1.p0.4/jars/hive-common-1.1.0-cdh5.15.1.jar!/hive-log4j.properties
OK
Time taken: 2.156 seconds
OK
Time taken: 0.347 seconds
OK
Time taken: 0.436 seconds
OK
Time taken: 0.396 seconds
OK
Time taken: 0.145 seconds
OK
Time taken: 0.39 seconds
OK
Time taken: 0.199 seconds
OK
Time taken: 0.499 seconds
OK
Time taken: 0.241 seconds
Loading data to table lacus.country_info
Table lacus.country_info stats: [numFiles=1, totalSize=3488]
OK
Time taken: 0.933 seconds
Loading data to table lacus.state_info
Table lacus.state_info stats: [numFiles=1, totalSize=1046]
OK
Time taken: 0.55 seconds
Loading data to table lacus.station_info
Table lacus.station_info stats: [numFiles=1, totalSize=5821405]
OK
Time taken: 0.606 seconds
+ echo '增加 Table Partition'
增加 Table Partition
+ hive -f alter_hive.sql
ls: cannot access /root/xiaoxiang/lib/spark-2.3.2//lib/spark-assembly-*.jar: No such file or directory
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0
Java HotSpot(TM) 64-Bit Server VM warning: Using incremental CMS is deprecated and will likely be removed in a future release
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0

Logging initialized using configuration in jar:file:/opt/cloudera/parcels/CDH-5.15.1-1.cdh5.15.1.p0.4/jars/hive-common-1.1.0-cdh5.15.1.jar!/hive-log4j.properties
OK
Time taken: 2.228 seconds
OK
Time taken: 0.447 seconds
+ echo $'\346\243\200\346\237\245\346\225\260\346\215\256'
检查数据
+ hive -f check_result.sql
ls: cannot access /root/xiaoxiang/lib/spark-2.3.2//lib/spark-assembly-*.jar: No such file or directory
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0
Java HotSpot(TM) 64-Bit Server VM warning: Using incremental CMS is deprecated and will likely be removed in a future release
Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=512M; support was removed in 8.0

Logging initialized using configuration in jar:file:/opt/cloudera/parcels/CDH-5.15.1-1.cdh5.15.1.p0.4/jars/hive-common-1.1.0-cdh5.15.1.jar!/hive-log4j.properties
OK
Time taken: 2.195 seconds
Query ID = root_20190319134242_d9a1ebe5-1864-498c-9335-64f1b2fa18e5
Total jobs = 2
Execution log at: /tmp/root/root_20190319134242_d9a1ebe5-1864-498c-9335-64f1b2fa18e5.log
2019-03-19 01:42:58	Starting to launch local task to process map join;	maximum memory = 1908932608
2019-03-19 01:42:59	Dump the side-table for tag: 1 with group count: 74 into file: file:/tmp/root/b7f90ea9-a20b-4866-a33b-73a2a236e1dd/hive_2019-03-19_13-42-52_323_5141141291282164553-1/-local-10007/HashTable-Stage-4/MapJoin-mapfile01--.hashtable
2019-03-19 01:42:59	Uploaded 1 File to: file:/tmp/root/b7f90ea9-a20b-4866-a33b-73a2a236e1dd/hive_2019-03-19_13-42-52_323_5141141291282164553-1/-local-10007/HashTable-Stage-4/MapJoin-mapfile01--.hashtable (1748 bytes)
2019-03-19 01:42:59	Dump the side-table for tag: 1 with group count: 219 into file: file:/tmp/root/b7f90ea9-a20b-4866-a33b-73a2a236e1dd/hive_2019-03-19_13-42-52_323_5141141291282164553-1/-local-10007/HashTable-Stage-4/MapJoin-mapfile11--.hashtable
2019-03-19 01:42:59	Uploaded 1 File to: file:/tmp/root/b7f90ea9-a20b-4866-a33b-73a2a236e1dd/hive_2019-03-19_13-42-52_323_5141141291282164553-1/-local-10007/HashTable-Stage-4/MapJoin-mapfile11--.hashtable (4663 bytes)
2019-03-19 01:42:59	Dump the side-table for tag: 1 with group count: 108081 into file: file:/tmp/root/b7f90ea9-a20b-4866-a33b-73a2a236e1dd/hive_2019-03-19_13-42-52_323_5141141291282164553-1/-local-10007/HashTable-Stage-4/MapJoin-mapfile21--.hashtable

2019-03-19 01:42:59	Uploaded 1 File to: file:/tmp/root/b7f90ea9-a20b-4866-a33b-73a2a236e1dd/hive_2019-03-19_13-42-52_323_5141141291282164553-1/-local-10007/HashTable-Stage-4/MapJoin-mapfile21--.hashtable (3831784 bytes)

Execution completed successfully
MapredLocal task succeeded
Launching Job 1 out of 2

import glob
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1550751025988_1413, Tracking URL = http://cdh1.cloudera.com:8088/proxy/application_1550751025988_1413/
Kill Command = /opt/cloudera/parcels/CDH-5.15.1-1.cdh5.15.1.p0.4/lib/hadoop/bin/hadoop job  -kill job_1550751025988_1413
Hadoop job information for Stage-4: number of mappers: 1; number of reducers: 1
2019-03-19 13:43:08,089 Stage-4 map = 0%,  reduce = 0%
2019-03-19 13:43:13,303 Stage-4 map = 100%,  reduce = 0%, Cumulative CPU 1.94 sec
2019-03-19 13:43:19,488 Stage-4 map = 100%,  reduce = 100%, Cumulative CPU 4.47 sec
MapReduce Total cumulative CPU time: 4 seconds 470 msec
Ended Job = job_1550751025988_1413
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1550751025988_1414, Tracking URL = http://cdh1.cloudera.com:8088/proxy/application_1550751025988_1414/
Kill Command = /opt/cloudera/parcels/CDH-5.15.1-1.cdh5.15.1.p0.4/lib/hadoop/bin/hadoop job  -kill job_1550751025988_1414
Hadoop job information for Stage-5: number of mappers: 1; number of reducers: 1
2019-03-19 13:43:27,982 Stage-5 map = 0%,  reduce = 0%
2019-03-19 13:43:33,135 Stage-5 map = 100%,  reduce = 0%, Cumulative CPU 1.48 sec
2019-03-19 13:43:39,320 Stage-5 map = 100%,  reduce = 100%, Cumulative CPU 3.58 sec
MapReduce Total cumulative CPU time: 3 seconds 580 msec
Ended Job = job_1550751025988_1414
MapReduce Jobs Launched:
Stage-Stage-4: Map: 1  Reduce: 1   Cumulative CPU: 4.47 sec   HDFS Read: 19724 HDFS Write: 96 SUCCESS
Stage-Stage-5: Map: 1  Reduce: 1   Cumulative CPU: 3.58 sec   HDFS Read: 7063 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 8 seconds 50 msec
OK
Time taken: 48.936 seconds
```