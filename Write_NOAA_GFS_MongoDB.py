import pygrib
import os
import pandas as pd
from datetime import datetime, timedelta
from telegram_notification import telegram_bot_send_message
import schedule
import time
from Anaconda.database import db_MongoDB
from multiprocessing import Pool

dict_in_index = {142: 432, 1: 0, 5: 0, 107: 1904, 24: 0, 183: 1380, 166: 1633, 126: 933, 161: 1549, 287: 1553, 20: 0, 108: 2142, 109: 2142, 110: 2142, 111: 2138, 180: 1099,
                 131: 1567, 125: 933, 17: 0, 280: 1011, 305: 1713, 136: 1333, 140: 2056, 72: 762, 7: 857, 250: 1431, 159: 2465, 163: 1549, 162: 1549, 16: 853, 165: 1631,
                 127: 1508, 251: 1709, 255: 927, 18: 0, 141: 1171, 135: 1011, 314: 1661, 313: 761, 15: 0, 288: 1393, 134: 514, 290: 1811, 211: 1012, 95: 1171, 96: 1092,
                 220: 2060, 2: 0, 4: 0, 6: 0, 222: 2394, 93: 1647, 185: 1380, 247: 937, 191: 1436, 3: 0, 219: 2060, 179: 1876, 285: 2468, 22: 0, 298: 1802, 301: 1802,
                 193: 1890, 171: 1549, 11: 0, 253: 1328, 254: 1247, 277: 2142, 281: 1870, 19: 681, 210: 1824, 21: 1560, 286: 2468, 23: 0, 229: 1631, 241: 1429, 8: 2142,
                 187: 1012, 32: 1489, 33: 0, 84: 1808, 91: 2471, 14: 1101, 192: 1509, 297: 1802, 256: 1506, 173: 1470, 174: 1548, 37: 0, 295: 1802, 184: 1380, 13: 1718,
                 261: 1484, 176: 1633, 178: 1807, 34: 0, 35: 0, 40: 0, 42: 843, 43: 843, 45: 1566, 46: 1811, 48: 1640, 214: 1299, 160: 2465, 168: 1551, 169: 1549, 170: 1631,
                 181: 1264, 182: 1264, 9: 0, 10: 0, 264: 1632, 190: 2301, 283: 2133, 291: 931, 27: 1650, 307: 1379, 304: 1952, 296: 1802, 299: 1802, 300: 1802, 302: 1171,
                 294: 1802, 293: 2392, 292: 1254, 50: 945, 195: 1012, 167: 1629, 172: 1468, 105: 1500, 106: 1499, 175: 1551, 225: 1013, 74: 1175, 69: 1477, 63: 1556, 68: 1480,
                 77: 1172, 226: 678, 47: 1557, 80: 1094, 81: 1409, 114: 1922, 186: 1012, 118: 1762, 198: 1570, 54: 1091, 117: 1672, 113: 1922, 79: 1332, 51: 1415, 70: 1013,
                 73: 762, 56: 1093, 75: 1965, 76: 1577, 57: 1172, 188: 2134, 275: 1800, 208: 933, 209: 1986, 41: 780, 128: 1727, 205: 1475, 53: 1409, 58: 1258, 143: 432,
                 154: 2317, 100: 1964, 236: 932, 90: 1095, 86: 1750, 132: 1822, 88: 598, 232: 1822, 121: 1267, 246: 1903, 123: 855, 260: 1722, 133: 1822, 137: 1632, 138: 1551,
                 239: 1182, 144: 2055, 146: 1640, 147: 1640, 148: 1640, 149: 1723, 150: 1723, 151: 1558, 152: 1551, 153: 598, 155: 1470, 157: 1312, 158: 2465, 164: 1712,
                 124: 855, 89: 1418, 82: 2048, 85: 1808, 87: 1989, 242: 1590, 189: 2205, 248: 1919, 249: 1557, 265: 1548, 266: 1410, 270: 1665, 194: 1550, 202: 1971, 217: 1459,
                 221: 2301, 223: 1583, 227: 853, 231: 1916, 243: 1672, 244: 1589, 245: 1551, 119: 1727, 257: 1799, 258: 1881, 259: 1881, 196: 2221, 197: 935, 199: 1092,
                 200: 1012, 201: 2061, 271: 1420, 272: 1420, 273: 1420, 274: 1420, 356: 1508, 357: 1415, 358: 0, 359: 1802, 213: 1628, 215: 1460, 216: 1459, 218: 1635,
                 177: 1821, 115: 433, 116: 1381, 55: 945, 59: 1172, 60: 1174, 62: 1719, 330: 2294, 331: 2375, 348: 1873, 204: 1461, 276: 1800, 206: 2476, 207: 1659, 156: 1312,
                 278: 1011, 332: 1720, 325: 1260, 326: 1260, 25: 1412, 333: 1802, 336: 1730, 337: 1730, 338: 928, 339: 1815, 340: 1815, 341: 1815, 52: 1410, 83: 1090, 49: 1171,
                 346: 926, 344: 1013, 351: 1488, 347: 1578, 343: 1707, 203: 761, 349: 1179, 350: 1579, 352: 1786, 355: 1257, 345: 926, 342: 1471, 92: 1583, 212: 1470, 94: 1485,
                 377: 1177, 378: 1579, 380: 1631, 384: 1557, 385: 1557, 382: 2376, 383: 2294
                 }


def date_time(validityTime, validityDate):
    # Формат дати та часу
    year = str(validityDate)[:4]
    month = str(validityDate)[4:6]
    day = str(validityDate)[6:]
    if len(str(validityTime)) == 3:
        validityTime = "0" + str(validityTime)
    if len(str(validityTime)) == 1:
        validityTime = "000" + str(validityTime)
    DateTime = f"{year}-{month}-{day} {str(validityTime)[:2]}:00:00"
    return DateTime


def value_data_in_list(parametr, dict_in_index):
    value = parametr["values"].tolist()
    parameterName = parametr["parameterName"]
    result = {}
    value_rez = []
    df_one_parametr = pd.DataFrame()
    for val in value:
        value_rez = value_rez + val
    for key in dict_in_index:
        index = dict_in_index[key]
        data = value_rez[index]
        result[key] = data
        df_one_id = pd.DataFrame()
        df_one_id['id_st'] = [key]
        df_one_id[parameterName] = data
        df_one_parametr = pd.concat([df_one_parametr, df_one_id], axis=0)
    return df_one_parametr


def parse_data_1_hour(grib_file_path, dict_in_index):
    instant_parametr_keys = [
        'High cloud cover',
        'Low cloud cover',
        'Medium cloud cover',
        'Relative humidity',
        'Temperature',
        'Visibility',
        'Wind speed (gust)',
        'Precipitable water',
        ]

    grib = pygrib.open(grib_file_path)
    matching_grids = []

    for message in grib:
        if "latitudes" in message.keys() and "longitudes" in message.keys():
            matching_grids.append(message)

    if not matching_grids:
        print("Не знайдено гридів з ключами 'latitudes' та 'longitudes'")

    result_df = pd.DataFrame()

    for parametr in matching_grids:

        if parametr['stepTypeInternal'] == 'instant':
            if parametr['parameterName'] in instant_parametr_keys:
                rez = value_data_in_list(parametr, dict_in_index).reset_index(drop=True)
                result_df['id_st'] = rez['id_st']
                if 'Temperature' in parametr["parameterName"] and parametr["level"] == 1:
                    result_df[parametr["parameterName"] + "_" + str(parametr["level"]) + " m"] = rez[parametr["parameterName"]]
                else:
                    if 'Temperature' not in parametr["parameterName"]:
                        result_df[parametr["parameterName"]] = rez[parametr["parameterName"]]
            elif 'component' in str(parametr) and parametr['typeOfLevel'] == 'heightAboveGround':
                rez = value_data_in_list(parametr, dict_in_index).reset_index(drop=True)
                result_df[parametr["parameterName"] + "_" + str(parametr["level"]) + " m"] = rez[parametr["parameterName"]]

            if parametr['parameterName'] == instant_parametr_keys[-1]:
                validityTime = parametr['validityTime']
                validityDate = parametr['validityDate']
                DateTime = date_time(validityTime, validityDate)
                parsed_date = datetime.strptime(DateTime, '%Y-%m-%d %H:%M:%S')
                if parsed_date.date() < pd.to_datetime('2024-03-31').date():
                    parsed_date = parsed_date + timedelta(hours=2)
                elif parsed_date.date() >= pd.to_datetime('2024-03-31').date():
                    parsed_date = parsed_date + timedelta(hours=3)
                result_df.insert(1, 'dt', pd.Timestamp(parsed_date))
    print(grib_file_path, 'OK, фортануло')
    return result_df


def parse_data_1_hour_wrapper(args):
    grib_file_path, dict_in_index = args
    return parse_data_1_hour(grib_file_path, dict_in_index)


def parse_data_rez_df(output_dir):
    print(datetime.now())
    files = os.listdir(output_dir)
    args_list = [(os.path.join(output_dir, file), dict_in_index) for file in files]

    with Pool() as pool:
        results = pool.map(parse_data_1_hour_wrapper, args_list)

    df = pd.concat(results, ignore_index=True)

    return df


def write_to_baza():

    meteo_df_all = parse_data_rez_df('D:\\ANACONDA\\NOAA\\GFS\\1-hour files')
    meteo_df_all['Temperature_1 m'] = meteo_df_all['Temperature_1 m'] - 273.15

    id_stations = list(set(meteo_df_all['id_st'].tolist()))

    for id_station in id_stations:
        print('id_station=', id_station)
        meteo_df = meteo_df_all[meteo_df_all['id_st'] == id_station].reset_index(drop=True)
        data = []
        for i in range(meteo_df.shape[0]):
            meteo_data = {'id_st': int(meteo_df['id_st'][i]),
                          'dt': meteo_df['dt'][i],
                          'rt': datetime.now(),
                          'params': {
                              'visibility': float(meteo_df['Visibility'][i]),
                              'wind_speed_gust': float(meteo_df['Wind speed (gust)'][i]),
                              'relative_humidity': float(meteo_df['Relative humidity'][i]),
                              'temperature_1m': float(meteo_df['Temperature_1 m'][i]),
                              'precipitable_water': float(meteo_df['Precipitable water'][i]),
                              'low_cloud_cover': float(meteo_df['Low cloud cover'][i]),
                              'medium_cloud_cover': float(meteo_df['Medium cloud cover'][i]),
                              'high_cloud_cover': float(meteo_df['High cloud cover'][i]),
                              'u_wind_10m': float(meteo_df['u-component of wind_10 m'][i]),
                              'v_wind_10m': float(meteo_df['v-component of wind_10 m'][i]),
                              'u_wind_20m': float(meteo_df['u-component of wind_20 m'][i]),
                              'v_wind_20m': float(meteo_df['v-component of wind_20 m'][i]),
                              'u_wind_30m': float(meteo_df['u-component of wind_30 m'][i]),
                              'v_wind_30m': float(meteo_df['v-component of wind_30 m'][i]),
                              'u_wind_40m': float(meteo_df['u-component of wind_40 m'][i]),
                              'v_wind_40m': float(meteo_df['v-component of wind_40 m'][i]),
                              'u_wind_50m': float(meteo_df['u-component of wind_50 m'][i]),
                              'v_wind_50m': float(meteo_df['v-component of wind_50 m'][i]),
                              'u_wind_80m': float(meteo_df['u-component of wind_80 m'][i]),
                              'v_wind_80m': float(meteo_df['v-component of wind_80 m'][i]),
                              'u_wind_100m': float(meteo_df['u-component of wind_100 m'][i]),
                              'v_wind_100m': float(meteo_df['v-component of wind_100 m'][i])}
                          }
            data.append(meteo_data)
        db_MongoDB.Write().set_noaa_gfs(data)
        print(datetime.now())


def run_main():
    a = datetime.now()
    print("Запуск успішний, початок о", a)
    print('Пішла жара, глянеш сіки часу займе -_*')
    write_to_baza()
    telegram_bot_send_message(f"⛈ Запис NOAA-GFS з Grib Files на реліз завершено! \nЧас завантаження:  {str(datetime.now().hour)} година, {str(datetime.now().date())}", silent=True)
    b = datetime.now()
    print("Запис успішний, завершився о", b)


def schedule_job():
    schedule.every().day.at("08:10").do(run_main)
    schedule.every().day.at("13:40").do(run_main)
    schedule.every().day.at("19:40").do(run_main)
    schedule.every().day.at("03:20").do(run_main)
    schedule.every().day.at("20:40").do(run_main)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    schedule_job()

