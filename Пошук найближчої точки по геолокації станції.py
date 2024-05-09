import pygrib
import os
import pandas as pd
from database import db2

station_info = db2.Read().get_stations_forecast_info()

# stations = station_info['id_st'].astype(int).tolist()

stations = [117]


def extract_data_for_coordinates_with_lat_lon_keys(grib_file_path, target_lat, target_lon):

    grib = pygrib.open(grib_file_path)

    matching_grids = []
    for message in grib:
        if "latitudes" in message.keys() and "longitudes" in message.keys():
            matching_grids.append(message)

    if not matching_grids:
        print("Не знайдено гридів з ключами 'latitudes' та 'longitudes'")
        return None

    for grid in matching_grids:
        min_distance = float("inf")
        index_pars = []
        latitudes = grid["latitudes"]
        longitudes = grid["longitudes"]
        value = grid["values"].tolist()
        value_rez = []
        for val in value:
            value_rez = value_rez+val
        index = 0
        while index < len(latitudes):
            grid_lat = latitudes[index]

            grid_lon = longitudes[index]
            distance = abs(target_lat - grid_lat) + abs(target_lon - grid_lon)
            if distance < min_distance:
                min_distance = distance
                index_pars = index
            index = index + 1

        return index_pars


def run():
    output_dir = 'D:\\ANACONDA\GFS\\'
    files = os.listdir(output_dir)
    for st in stations:

        target_lat = station_info['lat_st'].where(station_info['id_st'] == st).dropna().reset_index(drop=True)[0]
        target_lon = station_info['lon_st'].where(station_info['id_st'] == st).dropna().reset_index(drop=True)[0]


        for file in files:
            grib_file_path = f'{output_dir}\\{file}'
            index_pars = extract_data_for_coordinates_with_lat_lon_keys(grib_file_path, target_lat, target_lon)
            op = f'{st}: {index_pars}, '

            print(op)


run()











