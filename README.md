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
todo