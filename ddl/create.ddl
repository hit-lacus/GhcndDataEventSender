use lacus;

drop table if exists ghcn;
create table if not exists ghcn(
    station_id string comment "id of climate observation station, FK",
    obs_day date comment "observe date",
    obs_year INT comment "observe year",
    max_temperature double,
    min_temperature double,
    avg_temperature double,
    precipitation INT,
    snow_depth INT,
    snow_fall INT,
    snow_water_equivalent INT,
    avg_wind_speed INT,
    fastest_2_min_wind_speed INT,
    ts STRING comment "event send timestamp"
)
COMMENT "Fact Table, contains different measure of climate all over the world"
partitioned by (part_year string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/LacusDir/data/hive/ghcn/';


drop table if exists station_info;
create table if not exists station_info(
station_id string comment "PK",
latitude float,
longitude float,
elevation float,
country string,
us_state string,
station_name string
)
COMMENT "Dimension Table, contains climate station info"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;


drop table if exists country_info;
create table if not exists country_info(
name string comment "PK, name of a country",
full_name string
)
COMMENT "Dimension Table, contains country info"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;


drop table if exists state_info;
create table if not exists state_info(
name string comment "PK, name of a state of US",
full_name string
)
COMMENT "Dimension Table, contains USA state info"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;


LOAD DATA LOCAL INPATH '../country.data' OVERWRITE INTO TABLE country_info;
LOAD DATA LOCAL INPATH '../state.data' OVERWRITE INTO TABLE state_info;
LOAD DATA LOCAL INPATH '../station.data' OVERWRITE INTO TABLE station_info;






