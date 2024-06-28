import pygrib
import os
import pandas as pd
from datetime import datetime, timedelta
from database import db_MongoDB
import schedule
import time
from multiprocessing import Pool
from telegram_notification import telegram_bot_send_message

file_path = 'D:\\ANACONDA\\ECMWF\\20240520000000-0h-enfo-ef.grib2'
target_lat = 48.7
target_lon = 65.94

def extract_data_for_coordinates_with_lat_lon_keys(file_path, target_lat, target_lon):
    grib = pygrib.open(file_path)
    matching_grids = []

    for message in grib:
        if "latitudes" in message.keys() and "longitudes" in message.keys():
            matching_grids.append(message)
            print(message)

    if not matching_grids:
        print("Не знайдено гридів з ключами 'latitudes' та 'longitudes'")
        return None

    for grid in matching_grids:
        reliz = f'{grid["hour"]}'
        min_distance = float("inf")
        index_pars = []
        validityTime = grid['validityTime']
        validityDate = grid['validityDate']
        parameterName = grid["parameterName"]
        Date_pars = f'{grid["day"]}-{grid["month"]}-{grid["year"]} {grid["hour"]}:{grid["minute"]}'
        latitudes = grid["latitudes"]
        longitudes = grid["longitudes"]
        value = grid["values"].tolist()
        value_rez = []
        for val in value:
            value_rez = value_rez + val
        index = 0
        while index < len(latitudes):
            grid_lat = latitudes[index]
            grid_lon = longitudes[index]
            distance = abs(target_lat - grid_lat) + abs(target_lon - grid_lon)
            if distance < min_distance:
                min_distance = distance
                index_pars = index
            index = index + 1
        latitudes = latitudes[index_pars]
        longitudes = longitudes[index_pars]
        data = value_rez[index_pars]
        print(parameterName)
        print(Date_pars)
        print(latitudes)
        print(longitudes)
        print(data)
        print(reliz)

extract_data_for_coordinates_with_lat_lon_keys(file_path, target_lat, target_lon)
k = 12