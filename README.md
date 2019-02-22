# GhcndDataEventSender
This project's purpose is 
- Python script which used to send ghcnd data to mock a **real-time event data source**.
- _ghcnd_ stand for "GLOBAL HISTORICAL CLIMATOLOGY NETWORK DAILY"


### Where to download
- [Where to download](https://www.kaggle.com/noaa/noaa-global-historical-climatology-network-daily)
- [Official Documentation For Data](ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt)
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

### Data Schema
- [detail](./metadata/_ghcnd_readme.txt)
- [sample data](./sample.csv)


### Data Size
- 14.99 GB csv file in gzip.
- About 3 billion raw data.


### Script Usage
todo