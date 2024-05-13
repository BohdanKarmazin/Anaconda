import pygrib
import os
import pandas as pd
from datetime import datetime, timedelta
from multiprocessing import Pool
import timec

dict_in_index = {921: 422264, 922: 422265, 923: 422266, 924: 423640, 925: 423641, 926: 423642, 927: 423643, 928: 423644, 929: 425018, 9210: 425019, 9211: 425020,
                 471: 422163, 472: 422164, 473: 422165, 474: 423541
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
    output_base_dir = 'D:\\ANACONDA'
    parametr_list = ['clch', 'clcl', 'clcm', 'tot_prec']    # todo має бути обов'язково послідовність ['clch', 'clcl', 'clcm', 'tot_prec']
    num_processes = 4

    # rez_df = pd.DataFrame(columns=['id_st', 'dt', 'rt', 'low_cloud', 'medium_cloud', 'height_cloud', 'precipiration'])
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
        k = 123
        for key in dict_in_index:
            data_1_st = pd.DataFrame(columns=['id_st', 'dt', 'rt', 'clch', 'clcl', 'clcm', 'tot_prec'])
            data_1_st['dt'] = final_dataframes[0]['Дата та Час']
            data_1_st['id_st'] = key
            data_1_st['rt'] = datetime.now().strftime("%Y-%m-%d %H:00:00")
            for index, parametr in enumerate(parametr_list):
                parametr_id = f'{key}_{parametr}'
                data_1_st[parametr] = final_dataframes[index][parametr_id]
            rez_df = pd.concat([rez_df, data_1_st], axis=0)
    rez_df.to_excel(f'D:\ANACONDA\history\Погода_о_{datetime.now().date()}_{datetime.now().hour}.xlsx', index=False)

def run_main():
    print("Запуск успішний, початок о", datetime.now())
    print('Пішла жара, глянеш сіки часу займе -_*')
    write_to_baza()


if __name__ == '__main__':
    run_main()