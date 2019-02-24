# -*- coding: utf-8 -*-
import json


def read_state_us(path):
    state_dict = {}
    with open(path, 'rb') as pf:
        while True:
            text_line = pf.readline()
            if text_line:
                state_dict[text_line[0:2]] = text_line[3:-1].strip()
            else:
                break
    return state_dict


def read_country(path):
    country_dict = {}
    with open(path, 'rb') as pf:
        while True:
            text_line = pf.readline()
            if text_line:
                country_dict[text_line[0:2]] = text_line[3:-1].strip()
            else:
                break
    return country_dict


def read_station(path):
    station_dict = {}
    with open(path, 'rb') as pf:
        while True:
            text_line = pf.readline()
            if text_line:
                country = text_line[0:2].strip()
                station_id = text_line[0:12].strip()
                latitude = text_line[12:20].strip()
                longitude = text_line[21:30].strip()
                elevation = text_line[31:37].strip()
                us_state = text_line[38:40].strip()
                station_name = text_line[41:71].strip()
                station_dict[station_id] = [float(latitude), float(longitude), float(elevation), country, us_state,
                                            station_name]
            else:
                break
    return station_dict


if __name__ == "__main__":
    res = read_station('../metadata/ghcnd-stations.txt')
    print len(res)
    print json.dumps(res.values()[0: 20], indent=1)

    res = read_country('../metadata/ghcnd-countries.txt')
    print json.dumps(res.items()[0:20], indent=1)

    res = read_country('../metadata/ghcnd-states.txt')
    print json.dumps(res.items()[0:20], indent=1)
