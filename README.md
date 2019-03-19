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
[root@sandbox GhcndDataEventSender]# sh -x produce_hive_data.sh
+ cd source
+ rm -f 1874.data 1875.data 1876.data 1877.data 1878.data 1879.data country.data state.data station.data
+ python metadata_reader.py
+ ls -lh country.data state.data station.data
-rw-r--r-- 1 root root 3.5K Mar 12 03:27 country.data
-rw-r--r-- 1 root root 1.1K Mar 12 03:27 state.data
-rw-r--r-- 1 root root 5.6M Mar 12 03:27 station.data
+ python data_sender.py --data-path '/root/lacus/data/ghcnd/*.csv.gz' --sleep-millsecond-per-thousand 0 --output-format csv
To be read files count 6
[
 "/root/lacus/data/ghcnd/1878.csv.gz",
 "/root/lacus/data/ghcnd/1876.csv.gz",
 "/root/lacus/data/ghcnd/1874.csv.gz",
 "/root/lacus/data/ghcnd/1875.csv.gz",
 "/root/lacus/data/ghcnd/1879.csv.gz",
 "/root/lacus/data/ghcnd/1877.csv.gz"
]
>>>STATS<<< [/root/lacus/data/ghcnd/1878.csv.gz] : event count 271642, send 148893 msg, start from 2019-03-12 03:27:24.590353 ,duration is 0:00:03.236654, rate 46002.1367746 msg/sec
>>>STATS<<< [/root/lacus/data/ghcnd/1876.csv.gz] : event count 201369, send 103178 msg, start from 2019-03-12 03:27:27.827054 ,duration is 0:00:02.324655, rate 44384.2204542 msg/sec
>>>STATS<<< [/root/lacus/data/ghcnd/1874.csv.gz] : event count 166256, send 89795 msg, start from 2019-03-12 03:27:30.151782 ,duration is 0:00:01.930531, rate 46513.1096056 msg/sec
>>>STATS<<< [/root/lacus/data/ghcnd/1875.csv.gz] : event count 180225, send 94590 msg, start from 2019-03-12 03:27:32.082360 ,duration is 0:00:02.115084, rate 44721.6280772 msg/sec
>>>STATS<<< [/root/lacus/data/ghcnd/1879.csv.gz] : event count 307737, send 176266 msg, start from 2019-03-12 03:27:34.197489 ,duration is 0:00:03.895741, rate 45245.8210133 msg/sec
>>>STATS<<< [/root/lacus/data/ghcnd/1877.csv.gz] : event count 227811, send 117056 msg, start from 2019-03-12 03:27:38.093310 ,duration is 0:00:02.718856, rate 43053.401872 msg/sec
+ cd ../ddl
+ ls -lh '*.data'
ls: cannot access *.data: No such file or directory
+ sh -x upload_hdfs.sh
+ hadoop fs -rm -r /LacusDir/data/hive/ghcn/
19/03/12 03:27:42 INFO fs.TrashPolicyDefault: Namenode trash configuration: Deletion interval = 360 minutes, Emptier interval = 0 minutes.
Moved: 'hdfs://sandbox.hortonworks.com:8020/LacusDir/data/hive/ghcn' to trash at: hdfs://sandbox.hortonworks.com:8020/user/root/.Trash/Current
+ hadoop fs -mkdir -p /LacusDir/data/hive/ghcn/1874/
+ hadoop fs -mkdir -p /LacusDir/data/hive/ghcn/1875/
+ hadoop fs -mkdir -p /LacusDir/data/hive/ghcn/1876/
+ hadoop fs -mkdir -p /LacusDir/data/hive/ghcn/1877/
+ hadoop fs -copyFromLocal ../source/1874.data /LacusDir/data/hive/ghcn/1874/
+ hadoop fs -copyFromLocal ../source/1875.data /LacusDir/data/hive/ghcn/1875/
+ hadoop fs -copyFromLocal ../source/1876.data /LacusDir/data/hive/ghcn/1876/
+ hadoop fs -copyFromLocal ../source/1877.data /LacusDir/data/hive/ghcn/1877/
+ hive -f create.ddl
WARNING: Use "yarn jar" to launch YARN applications.

Logging initialized using configuration in file:/etc/hive/2.4.0.0-169/0/hive-log4j.properties
OK
Time taken: 1.313 seconds
OK
Time taken: 0.732 seconds
pwd
OK
Time taken: 5.401 seconds
OK
Time taken: 0.702 seconds
OK
Time taken: 0.212 seconds
OK
Time taken: 0.448 seconds
OK
Time taken: 0.237 seconds
OK
Time taken: 0.453 seconds
OK
Time taken: 0.242 seconds
Loading data to table lacus.country_info
Table lacus.country_info stats: [numFiles=1, totalSize=3488]
OK
Time taken: 0.806 seconds
Loading data to table lacus.state_info
Table lacus.state_info stats: [numFiles=1, totalSize=1046]
OK
Time taken: 0.803 seconds
Loading data to table lacus.station_info
Table lacus.station_info stats: [numFiles=1, totalSize=5821405]
OK
Time taken: 0.977 seconds
OK
Time taken: 0.87 seconds
OK
Time taken: 0.452 seconds
OK
Time taken: 0.453 seconds
OK
Time taken: 0.454 seconds
OK
Time taken: 0.258 seconds
OK
Time taken: 0.349 seconds
OK
Time taken: 0.343 seconds
OK
Time taken: 0.37 seconds
Query ID = root_20190312032826_474f53e3-22ba-4fc1-82ef-063e46d109aa
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1552311794083_0023)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      1          1        0        0       0       0
Map 4 ..........   SUCCEEDED      1          1        0        0       0       0
Map 5 ..........   SUCCEEDED      1          1        0        0       0       0
Map 6 ..........   SUCCEEDED      1          1        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
Reducer 3 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 06/06  [==========================>>] 100%  ELAPSED TIME: 7.28 s
--------------------------------------------------------------------------------
OK
1874	CA	37.8	1270	20123	70
1874	US	43.9	1880	16601	50
1875	CA	36.7	1120	21907	66
1875	US	38.9	2159	17094	54
1876	CA	38.3	1732	23237	71
1876	US	40.6	1981	17193	56
1877	CA	42.8	1321	26716	81
1877	US	42.2	2286	17017	60
```