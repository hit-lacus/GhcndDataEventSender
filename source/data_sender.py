import gzip
import metadata_reader as METADATA
import json
from datetime import datetime

COUNTRY_DICT = METADATA.read_country('../metadata/ghcnd-countries.txt')

STATE_DICT = METADATA.read_state_us('../metadata/ghcnd-states.txt')

STATION_DICT = METADATA.read_station('../metadata/ghcnd-stations.txt')

ELEMENT_DICT = {'PRCP': 'precipitation', 'SNOW': 'snow_fall', 'SNWD': 'snow_depth', 'TMAX': 'max_temperature',
                'TMIN': 'min_temperature', 'TAVG': 'avg_temperature', 'WESD': 'snow_water_equivalent',
                'AWND': 'avg_wind_speed', 'WSF2': 'fastest_2_min_wind_speed'}

ELEMENT_TYPE = set(ELEMENT_DICT.keys())

USE_NULL = True

msg = dict()


def transform_degree(degree):
    if degree is None:
        return degree
    elif degree == 1000 or degree == -1000:
        return degree + 0.0
    else:
        return round((degree - 32) / 1.8, 2)


class StationDailyMsg:

    def __init__(self, station_id, date_str):
        self.station_id = station_id
        self.date_str = date_str

        # Dimsension
        self.station_name = ''
        self.year = int(date_str[0:4])
        self.month = int(date_str[4:6])
        self.day = int(date_str[6:8])
        if USE_NULL:
            self.country = None
            self.country_name = None
            self.us_state = None
            self.state_name = None
            self.latitude = None
            self.longitude = None
            self.elevation = None
            # Measure with null
            self.max_temperature = None
            self.min_temperature = None
            self.avg_temperature = None
            self.precipitation = None
            self.snow_depth = None
            self.snow_fall = None
            self.snow_water_equivalent = None
            self.avg_wind_speed = None
            self.fastest_2_min_wind_speed = None
        else:
            self.country = ''
            self.country_name = ''
            self.us_state = ''
            self.state_name = ''
            self.latitude = -1.0
            self.longitude = -1.0
            self.elevation = -1.0
            # Measure with default value
            self.max_temperature = -1000.0
            self.min_temperature = 1000.0
            self.avg_temperature = -1000.0
            self.precipitation = 0
            self.snow_depth = 0
            self.snow_fall = 0
            self.snow_water_equivalent = 0
            self.avg_wind_speed = 0
            self.fastest_2_min_wind_speed = 0

    def enrich_msg(self):
        station_info = STATION_DICT.get(self.station_id)
        self.latitude = station_info[0]
        self.longitude = station_info[1]
        self.elevation = station_info[2]
        self.country = station_info[3]
        self.us_state = station_info[4]
        self.station_name = station_info[5]
        if COUNTRY_DICT.get(self.country) is not None:
            self.country_name = COUNTRY_DICT.get(self.country)
        if STATE_DICT.get(self.us_state) is not None:
            self.state_name = STATE_DICT.get(self.us_state)

    def __str__(self):
        msg['station_id'] = self.station_id
        msg['date'] = self.date_str
        msg['station_name'] = self.station_name
        msg['year'] = self.year
        msg['month'] = self.month
        msg['day'] = self.day
        msg['country'] = self.country
        msg['country_name'] = self.country_name
        msg['us_state'] = self.us_state
        msg['state_name'] = self.state_name
        msg['latitude'] = self.latitude
        msg['longitude'] = self.longitude
        msg['elevation'] = self.elevation
        msg['max_temperature'] = transform_degree(self.max_temperature)
        msg['min_temperature'] = transform_degree(self.min_temperature)
        msg['avg_temperature'] = transform_degree(self.avg_temperature)
        msg['precipitation'] = self.precipitation
        msg['snow_depth'] = self.snow_depth
        msg['snow_fall'] = self.snow_fall
        msg['snow_water_equivalent'] = self.snow_water_equivalent
        msg['avg_wind_speed'] = self.avg_wind_speed
        msg['fastest_2_min_wind_speed'] = self.fastest_2_min_wind_speed
        return json.dumps(msg)


def read_gzip_data_file(path):
    event_count = 0
    result_count = 0
    start_time = datetime.now()

    def process_station_events(s_id, events):
        if events and len(events) > 0:
            smsg = StationDailyMsg(s_id, events[0][0])
            smsg.enrich_msg()
            for event in events:
                et = event[1]
                if et == 'PRCP':
                    smsg.precipitation = int(event[2])
                elif et == 'TMAX':
                    smsg.max_temperature = int(event[2])
                elif et == 'TMIN':
                    smsg.min_temperature = int(event[2])
                elif et == 'TAVG':
                    smsg.avg_temperature = int(event[2])
                elif et == 'SNWD':
                    smsg.snow_depth = int(event[2])
                elif et == 'SNOW':
                    smsg.snow_fall = int(event[2])
                elif et == 'WESD':
                    smsg.snow_water_equivalent = int(event[2])
                elif et == 'AWND':
                    smsg.avg_wind_speed = int(event[2])
                elif et == 'WSF2':
                    smsg.fastest_2_min_wind_speed = int(event[2])
            print smsg
            return smsg
        else:
            return None

    with gzip.open(path, 'rb') as pf:
        curr_station_id = None
        single_station_event = []
        while True:
            text_line = pf.readline()
            if text_line:
                event_count += 1
                columns = text_line.split(',')
                element_type = columns[2]
                station_id = columns[0]
                datetime_str = columns[1]
                element_value = columns[3]
                if element_type not in ELEMENT_TYPE:
                    continue
                # measure_flag = columns[4]
                # quality_flag = columns[5]
                # source_flag = columns[6]
                # observation_hour_minute = columns[7]
                if station_id == curr_station_id:
                    single_station_event.append((datetime_str, element_type, element_value))
                else:
                    result_count += 1
                    process_station_events(curr_station_id, single_station_event)
                    curr_station_id = station_id
                    single_station_event = list()
                    single_station_event.append((datetime_str, element_type, element_value))
            else:
                break
    return event_count, result_count, datetime.now() - start_time


if __name__ == "__main__":
    print read_gzip_data_file('../sample/1872.csv.gz')
