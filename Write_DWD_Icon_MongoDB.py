import pygrib
import os
import pandas as pd
from datetime import datetime, timedelta
from Anaconda.database import db_MongoDB
import schedule
import time
from multiprocessing import Pool
from telegram_notification import telegram_bot_send_message

dict_in_index = {1: 376, 5: 376, 24: 376, 7: 374082, 15: 376, 16: 378198, 17: 376, 18: 376, 20: 376, 107: 444287, 108: 466296, 109: 466296, 110: 466296, 111: 464907, 125: 379573, 126: 379573, 127: 422291, 131: 424957,
                 134: 354727, 135: 389199, 136: 405714, 140: 458017, 141: 398829, 142: 350596, 159: 485572, 161: 424883, 162: 424883, 163: 424883, 165: 427642, 72: 367143, 166: 429029, 180: 391980, 183: 413837,
                 199: 389199, 211: 386450, 314: 430515, 313: 371270, 305: 437287, 250: 414042, 251: 437271, 290: 442859, 288: 415268, 287: 423523, 255: 378171, 280: 389199, 32: 419459, 2: 376, 4: 376, 6: 376,
                 96: 391953, 3: 376, 8: 466296, 11: 376, 14: 391988, 91: 485596, 93: 433213, 95: 400207, 19: 364389, 21: 424929, 22: 376, 23: 376, 33: 376, 171: 424884, 187: 387825, 188: 463514, 189: 468978,
                 191: 414064, 192: 420919, 193: 446984, 197: 379577, 84: 440095, 200: 386446, 201: 459414, 210: 442914, 219: 460784, 220: 460784, 222: 480104, 229: 429018, 297: 440071, 298: 440071, 301: 440071, 241: 412657,
                 247: 380963, 253: 405694, 285: 488336, 286: 488336, 254: 402939, 277: 466296, 281: 449659, 179: 446927, 256: 419528, 214: 411085, 37: 376, 294: 440071, 9: 376, 10: 376, 13: 433176, 174: 427632,
                 176: 429026, 27: 430471, 34: 376, 35: 376, 40: 376, 42: 375405, 43: 375405, 45: 426327, 46: 441485, 48: 431811, 50: 382371, 160: 485572, 167: 427633, 168: 427646, 169: 427640, 170: 429019, 173: 419386,
                 178: 441467, 181: 401631, 182: 401630, 184: 416593, 185: 416593, 196: 467668, 195: 387825, 307: 412461, 304: 451040, 302: 400207, 295: 440071, 296: 440071, 299: 440071, 300: 440071, 293: 478720, 292: 401593,
                 291: 383695, 283: 462133, 261: 420819, 264: 430398, 190: 475924, 128: 435964, 106: 416748, 55: 382371, 58: 404362, 59: 397457, 60: 396087, 62: 438688, 79: 408462, 175: 427646, 81: 413956, 53: 413956,
                 113: 448488, 114: 448488, 117: 430561, 118: 436104, 41: 372722, 47: 422164, 51: 415357, 54: 393324, 56: 390579, 57: 396079, 63: 423536, 68: 419426, 69: 422164, 70: 389204, 73: 367143, 74: 397469, 75: 455222,
                 76: 422244, 77: 397455, 80: 391960, 172: 422130, 186: 386449, 198: 426347, 205: 422159, 208: 380949, 209: 455305, 225: 387827, 226: 364376, 275: 441439, 276: 441439, 105: 416748, 154: 475988, 217: 417962,
                 232: 442905, 89: 412616, 87: 452565, 90: 391966, 100: 453841, 119: 435964, 121: 403021, 123: 374073, 124: 374073, 132: 442905, 133: 444282, 137: 429022, 138: 427644, 143: 350596, 144: 458011, 146: 427676,
                 147: 427676, 148: 427676, 149: 435947, 150: 435947, 151: 426296, 152: 422140, 153: 361625, 155: 419385, 157: 408384, 158: 485572, 164: 433149, 82: 457982, 194: 427642, 85: 440095, 202: 451113, 86: 437434,
                 236: 380945, 239: 398871, 242: 423673, 246: 448412, 248: 448478, 249: 422161, 260: 438697, 265: 424880, 266: 415337, 270: 429155, 88: 361625, 215: 419344, 216: 417962, 213: 429008, 218: 429036, 231: 449841,
                 243: 430559, 244: 427796, 245: 427647, 257: 438683, 258: 445571, 259: 444196, 271: 411245, 272: 411245, 273: 411245, 274: 411245, 227: 378198, 223: 423644, 221: 475924, 325: 402992, 326: 402992, 92: 423642,
                 94: 419444, 115: 349219, 116: 412465, 25: 413970, 49: 400207, 52: 415338, 156: 408384, 177: 440147, 83: 394697, 203: 371270, 204: 417971, 206: 486992, 207: 430510, 212: 419385, 278: 387823, 336: 435975, 337: 435975,
                 338: 379552, 339: 442876, 340: 442876, 341: 442876, 342: 419390, 343: 434508, 344: 387828, 345: 378167, 346: 378167, 348: 448293, 359: 441449, 377: 396101, 378: 423626, 380: 429018, 384: 424918, 385: 424918,
                 382: 478653, 383: 475895
                 }


def extract_data_for_coordinates_with_lat_lon_keys(grib_file_path, dict_in_index):
    grib = pygrib.open(grib_file_path)
    matching_grids = []
    for message in grib:
        if "latitudes" in message.keys() and "longitudes" in message.keys():
            matching_grids.append(message)

    if not matching_grids:
        print("Не знайдено гридів з ключами 'latitudes' та 'longitudes'")
        return None
    for grid in matching_grids:
        reliz = f'{grid["hour"]}'
        parameterName = grid["parameterName"]
        value = grid["values"].tolist()
        value_rez = []
        for val in value:
            value_rez = value_rez+val
        validityTime = grid['validityTime']
        validityDate = grid['validityDate']
        results = {}
        for key in dict_in_index:
            index = dict_in_index[key]
            data = value_rez[index]
            results[index] = data
        return results, parameterName, validityTime, validityDate, reliz


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



def parse_data_1_parametr(parametr, output_dir):
    files = os.listdir(output_dir)
    df_data = pd.DataFrame(columns=['Дата та Час'])
    list_data = []
    list_validityDateTime = []
    data = {}
    for file in files:
        grib_file_path = f'{output_dir}\\{file}'
        results, parameterName, validityTime, validityDate, reliz = extract_data_for_coordinates_with_lat_lon_keys(grib_file_path, dict_in_index)
        DateTime = date_time(validityTime, validityDate)
        parsed_date = datetime.strptime(DateTime, '%Y-%m-%d %H:%M:%S')
        list_validityDateTime.append(parsed_date)
        list_data.append(results)
        print("Фартануло")
    for d in list_data:
        for key, value in d.items():
            if key in data:
                data[key].append(value)
            else:
                data[key] = [value]

    df_data['Дата та Час'] = list_validityDateTime

    for key in dict_in_index:
        index = dict_in_index[key]
        name_id = f'{key}_{parametr}'
        df = pd.DataFrame(data[index], columns=[name_id])
        df_data = pd.concat([df_data, df], axis=1)

    return df_data


def write_to_baza():
    output_base_dir = 'D:\\ANACONDA\\ICON'
    parametr_list = ['clch', 'clcl', 'clcm', 'tot_prec']    # todo має бути обов'язково послідовність ['clch', 'clcl', 'clcm', 'tot_prec']
    num_processes = 4
    rez_df = pd.DataFrame(columns=['id_st', 'dt', 'rt', 'clch', 'clcl', 'clcm', 'tot_prec'])

    with Pool(processes=num_processes) as pool:
        results = []

        for parametr in parametr_list:
            output_dir = os.path.join(output_base_dir, parametr, 'Parse_ICON_Grib')
            result = pool.apply_async(parse_data_1_parametr, args=(parametr, output_dir))
            results.append(result)

        pool.close()
        pool.join()

        final_dataframes = [result.get() for result in results]
        for key in dict_in_index:
            data_1_st = pd.DataFrame(columns=['id_st', 'dt', 'rt', 'clch', 'clcl', 'clcm', 'tot_prec'])
            data_1_st['dt'] = final_dataframes[0]['Дата та Час']
            data_1_st['id_st'] = key
            data_1_st['rt'] = datetime.now().strftime("%Y-%m-%d %H:00:00")
            for index, parametr in enumerate(parametr_list):
                parametr_id = f'{key}_{parametr}'
                data_1_st[parametr] = final_dataframes[index][parametr_id]
            rez_df = pd.concat([rez_df, data_1_st], axis=0)
    rez_df = rez_df.reset_index(drop=True)
    data = []
    for i in range(rez_df.shape[0]):
        meteo_data = {'id_st': rez_df['id_st'][i],
                      'dt': rez_df['dt'][i] + timedelta(hours=2),
                      'rt': datetime.now(),
                      'params': {
                          'medium_cloud': rez_df['clcm'][i],
                          'low_cloud': rez_df['clcl'][i],
                          'height_cloud': rez_df['clch'][i],
                          'precipiration': rez_df['tot_prec'][i]}
                      }
        data.append(meteo_data)
    db_MongoDB.Write().set_dwd_icon(data)

def run_main():
    print("Запуск успішний, початок о", datetime.now())
    print('Пішла жара, глянеш сіки часу займе -_*')
    write_to_baza()
    telegram_bot_send_message(f"⛈ Запис DWD-Icon з Grib Files на реліз завершено! \nЧас завантаження:  {str(datetime.now().hour)} година, {str(datetime.now().date())}", silent=True)

def schedule_job():
    schedule.every().day.at("06:53").do(run_main)
    schedule.every().day.at("09:03").do(run_main)
    schedule.every().day.at("12:53").do(run_main)
    schedule.every().day.at("15:03").do(run_main)
    schedule.every().day.at("18:53").do(run_main)
    schedule.every().day.at("21:03").do(run_main)
    schedule.every().day.at("00:53").do(run_main)
    schedule.every().day.at("03:03").do(run_main)
    schedule.every().day.at("20:36").do(run_main)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    schedule_job()