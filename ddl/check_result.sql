use lacus;
SELECT part_year, country_info.name as country_name, max(max_temperature) as max_temperature,
    max(precipitation) as precipitation, count(*) as obs_count,
    count(distinct ghcn.station_id) as station_count
FROM ghcn
     LEFT JOIN station_info ON ghcn.station_id = station_info.station_id
     LEFT JOIN country_info ON station_info.country = country_info.name
     LEFT JOIN state_info ON station_info.us_state = state_info.name
GROUP BY part_year, country_info.name
ORDER BY part_year, country_info.name;