# GhcndDataEventSender
> This project's purpose is to provided a python script which used to mock a **real-time event data source**. _ghcnd_ stand for "GLOBAL HISTORICAL CLIMATOLOGY NETWORK DAILY"


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

### Event Schema

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


### How to use
* Prepare raw data from **where to download**

Here, "/root/xiaoxiang/data/ghcnd" path store raw file, and it is the value of `data-path` in streaming way and `raw_file` in batch way.

```sh
[root@cdh8 ghcnd]# pwd
/root/xiaoxiang/data/ghcnd
[root@cdh8 ghcnd]# ls
1763.csv.gz  1776.csv.gz  1789.csv.gz  1802.csv.gz  1815.csv.gz  1828.csv.gz  1841.csv.gz  1854.csv.gz  1867.csv.gz  1880.csv.gz  1893.csv.gz  1906.csv.gz  1919.csv.gz  1932.csv.gz  1945.csv.gz  1958.csv.gz  1971.csv.gz  1984.csv.gz  1997.csv.gz  2010.csv.gz
1764.csv.gz  1777.csv.gz  1790.csv.gz  1803.csv.gz  1816.csv.gz  1829.csv.gz  1842.csv.gz  1855.csv.gz  1868.csv.gz  1881.csv.gz  1894.csv.gz  1907.csv.gz  1920.csv.gz  1933.csv.gz  1946.csv.gz  1959.csv.gz  1972.csv.gz  1985.csv.gz  1998.csv.gz  2011.csv.gz
1765.csv.gz  1778.csv.gz  1791.csv.gz  1804.csv.gz  1817.csv.gz  1830.csv.gz  1843.csv.gz  1856.csv.gz  1869.csv.gz  1882.csv.gz  1895.csv.gz  1908.csv.gz  1921.csv.gz  1934.csv.gz  1947.csv.gz  1960.csv.gz  1973.csv.gz  1986.csv.gz  1999.csv.gz  2012.csv.gz
1766.csv.gz  1779.csv.gz  1792.csv.gz  1805.csv.gz  1818.csv.gz  1831.csv.gz  1844.csv.gz  1857.csv.gz  1870.csv.gz  1883.csv.gz  1896.csv.gz  1909.csv.gz  1922.csv.gz  1935.csv.gz  1948.csv.gz  1961.csv.gz  1974.csv.gz  1987.csv.gz  2000.csv.gz  2013.csv.gz
1767.csv.gz  1780.csv.gz  1793.csv.gz  1806.csv.gz  1819.csv.gz  1832.csv.gz  1845.csv.gz  1858.csv.gz  1871.csv.gz  1884.csv.gz  1897.csv.gz  1910.csv.gz  1923.csv.gz  1936.csv.gz  1949.csv.gz  1962.csv.gz  1975.csv.gz  1988.csv.gz  2001.csv.gz  2014.csv.gz
1768.csv.gz  1781.csv.gz  1794.csv.gz  1807.csv.gz  1820.csv.gz  1833.csv.gz  1846.csv.gz  1859.csv.gz  1872.csv.gz  1885.csv.gz  1898.csv.gz  1911.csv.gz  1924.csv.gz  1937.csv.gz  1950.csv.gz  1963.csv.gz  1976.csv.gz  1989.csv.gz  2002.csv.gz  2015.csv.gz
1769.csv.gz  1782.csv.gz  1795.csv.gz  1808.csv.gz  1821.csv.gz  1834.csv.gz  1847.csv.gz  1860.csv.gz  1873.csv.gz  1886.csv.gz  1899.csv.gz  1912.csv.gz  1925.csv.gz  1938.csv.gz  1951.csv.gz  1964.csv.gz  1977.csv.gz  1990.csv.gz  2003.csv.gz  2016.csv.gz
1770.csv.gz  1783.csv.gz  1796.csv.gz  1809.csv.gz  1822.csv.gz  1835.csv.gz  1848.csv.gz  1861.csv.gz  1874.csv.gz  1887.csv.gz  1900.csv.gz  1913.csv.gz  1926.csv.gz  1939.csv.gz  1952.csv.gz  1965.csv.gz  1978.csv.gz  1991.csv.gz  2004.csv.gz  2017.csv.gz
1771.csv.gz  1784.csv.gz  1797.csv.gz  1810.csv.gz  1823.csv.gz  1836.csv.gz  1849.csv.gz  1862.csv.gz  1875.csv.gz  1888.csv.gz  1901.csv.gz  1914.csv.gz  1927.csv.gz  1940.csv.gz  1953.csv.gz  1966.csv.gz  1979.csv.gz  1992.csv.gz  2005.csv.gz  2018.csv.gz
1772.csv.gz  1785.csv.gz  1798.csv.gz  1811.csv.gz  1824.csv.gz  1837.csv.gz  1850.csv.gz  1863.csv.gz  1876.csv.gz  1889.csv.gz  1902.csv.gz  1915.csv.gz  1928.csv.gz  1941.csv.gz  1954.csv.gz  1967.csv.gz  1980.csv.gz  1993.csv.gz  2006.csv.gz  2019.csv.gz
1773.csv.gz  1786.csv.gz  1799.csv.gz  1812.csv.gz  1825.csv.gz  1838.csv.gz  1851.csv.gz  1864.csv.gz  1877.csv.gz  1890.csv.gz  1903.csv.gz  1916.csv.gz  1929.csv.gz  1942.csv.gz  1955.csv.gz  1968.csv.gz  1981.csv.gz  1994.csv.gz  2007.csv.gz
1774.csv.gz  1787.csv.gz  1800.csv.gz  1813.csv.gz  1826.csv.gz  1839.csv.gz  1852.csv.gz  1865.csv.gz  1878.csv.gz  1891.csv.gz  1904.csv.gz  1917.csv.gz  1930.csv.gz  1943.csv.gz  1956.csv.gz  1969.csv.gz  1982.csv.gz  1995.csv.gz  2008.csv.gz
1775.csv.gz  1788.csv.gz  1801.csv.gz  1814.csv.gz  1827.csv.gz  1840.csv.gz  1853.csv.gz  1866.csv.gz  1879.csv.gz  1892.csv.gz  1905.csv.gz  1918.csv.gz  1931.csv.gz  1944.csv.gz  1957.csv.gz  1970.csv.gz  1983.csv.gz  1996.csv.gz  2009.csv.gz 
```

* Following script to send streaming message to somewhere (maybe kafka producer)
```sh
[root@cdh8 source]# python data_sender.py -help
usage: data_sender.py [-h]
                      [--sleep-millsecond-per-thousand SLEEP_MILLSECOND_PER_THOUSAND]
                      [--data-path DATA_PATH] [--enable-null-value]
                      [--display-sample] [--output-format OUTPUT_FORMAT]
                      [--forever FOREVER]
data_sender.py: error: argument -h/--help: ignored explicit argument 'elp'
[root@cdh8 source]# python data_sender.py -h
usage: data_sender.py [-h]
                      [--sleep-millsecond-per-thousand SLEEP_MILLSECOND_PER_THOUSAND]
                      [--data-path DATA_PATH] [--enable-null-value]
                      [--display-sample] [--output-format OUTPUT_FORMAT]
                      [--forever FOREVER]

optional arguments:
  -h, --help            show this help message and exit
  --sleep-millsecond-per-thousand SLEEP_MILLSECOND_PER_THOUSAND
                        After send one thousand msg, thread will sleep some
                        millseconds
  --data-path DATA_PATH
                        where does *.csv.gz store
  --enable-null-value   for some measure which does not exist, use null or
                        default value
  --display-sample      if you do not download required data file, you can use
                        provided sample file(../sample/1872.csv.gz)
  --output-format OUTPUT_FORMAT
                        In which format will program output, json or csv.
  --forever FOREVER     In streaming way, will message be sent forever?

[root@cdh8 source]# python data_sender.py
To be read files count 1
[
 "../sample/1872.csv.gz"
]
{"fastest_2_min_wind_speed": 0, "state_name": "ONTARIO", "max_temperature": -1000.0, "snow_fall": 0, "observe_date": "1872-1-1", "ts": "CA006144470-1-0-47", "latitude": 42, "min_temperature": 1000.0, "obs_day": 1, "obs_month": 1, "snow_depth": 0, "elevation": 200, "obs_year": 1872, "elevation_accurate": 246, "station_id": "CA006144470", "country_name": "Canada", "station_name": "LONDON", "precipitation": 0, "avg_temperature": -1000.0, "country": "CA", "longitude": -81, "snow_water_equivalent": 0, "us_state": "ON", "avg_wind_speed": 0}

...


```


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