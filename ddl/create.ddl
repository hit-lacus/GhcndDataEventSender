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
    ts BIGINT comment "event send timestamp"
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
name string comment "PK",
full_name string
)
COMMENT "Dimension Table, contains country info"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;


drop table if exists state_info;
create table if not exists state_info(
name string comment "PK",
full_name string
)
COMMENT "Dimension Table, contains USA state info"
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;


LOAD DATA LOCAL INPATH '../source/country.data' OVERWRITE INTO TABLE country_info;
LOAD DATA LOCAL INPATH '../source/state.data' OVERWRITE INTO TABLE state_info;
LOAD DATA LOCAL INPATH '../source/station.data' OVERWRITE INTO TABLE station_info;

ALTER TABLE ghcn DROP IF EXISTS PARTITION (part_year='1874');
ALTER TABLE ghcn DROP IF EXISTS PARTITION (part_year='1875');
ALTER TABLE ghcn DROP IF EXISTS PARTITION (part_year='1876');
ALTER TABLE ghcn DROP IF EXISTS PARTITION (part_year='1877');


ALTER TABLE ghcn ADD IF NOT EXISTS PARTITION (part_year='1874') LOCATION '/LacusDir/data/hive/ghcn/1874/';
ALTER TABLE ghcn ADD IF NOT EXISTS PARTITION (part_year='1875') LOCATION '/LacusDir/data/hive/ghcn/1875/';
ALTER TABLE ghcn ADD IF NOT EXISTS PARTITION (part_year='1876') LOCATION '/LacusDir/data/hive/ghcn/1876/';
ALTER TABLE ghcn ADD IF NOT EXISTS PARTITION (part_year='1877') LOCATION '/LacusDir/data/hive/ghcn/1877/';

SELECT part_year, country_info.name as country_name, max(max_temperature) as max_temperature,
    max(precipitation) as precipitation, count(*) as obs_count,
    count(distinct ghcn.station_id) as station_count
FROM ghcn
     JOIN station_info ON ghcn.station_id = station_info.station_id
     JOIN country_info ON station_info.country = country_info.name
     JOIN state_info ON station_info.us_state = state_info.name
GROUP BY part_year, country_info.name
ORDER BY part_year, country_info.name;




