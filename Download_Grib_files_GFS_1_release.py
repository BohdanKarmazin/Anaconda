from datetime import datetime, timedelta
import ssl
import os
import urllib.request
import concurrent.futures

select_date = (datetime.now()).strftime("%Y%m%d")#select_date = (datetime.now() ).strftime("%Y%m%d")#


one_hour_keys = ['000', '001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013', '014',
                  '015', '016', '017', '018', '019', '020', '021', '022', '023', '024', '025', '026', '027', '028', '029',
                  '030', '031', '032', '033', '034', '035', '036', '037', '038', '039', '040', '041', '042', '043', '044',
                  '045', '046', '047', '048', '049', '050', '051', '052', '053', '054', '055', '056', '057', '058', '059',
                  '060', '061', '062', '063', '064', '065', '066', '067', '068', '069', '070', '071', '072', '073', '074',
                  '075', '076', '077', '078', '079', '080', '081', '082', '083', '084', '085', '086', '087', '088', '089',
                  '090', '091', '092', '093', '094', '095', '096', '097', '098', '099', '100', '101', '102', '103', '104',
                  '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116', '117', '118', '119', '120']


three_hour_keys = ['123', '126', '129', '132', '135', '138', '141', '144', '147', '150', '153', '156', '159', '162', '165',
                    '168', '171', '174', '177', '180', '183', '186', '189', '192', '195', '198', '201', '204', '207', '210',
                    '213', '216', '219', '222', '225', '228', '231', '234', '237', '240', '243', '246', '249', '252', '255',
                    '258', '261', '264', '267', '270', '273', '276', '279', '282', '285', '288', '291', '294', '297', '300',
                    '303', '306', '309', '312', '315', '318', '321', '324', '327', '330', '333', '336', '339', '342', '345',
                    '348', '351', '354', '357', '360', '363', '366', '369', '372', '375', '378', '381', '384']




def download_files(type_keys, select_date, release):

    if type_keys == one_hour_keys:
        destination_folder = 'D:\\ANACONDA\\NOAA\\GFS\\1-hour files'

    elif type_keys == three_hour_keys:
        destination_folder = 'D:\\ANACONDA\\NOAA\\GFS\\3-hour files'

    files = os.listdir(destination_folder)
    for file_name in files:
        file = os.path.join(destination_folder, file_name)
        if os.path.isfile(file):
            os.remove(file)
    print(f'Усі файли в папці {destination_folder} були видалені.')

    def download_file(hour_key):
        try:
            url = f'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=%2Fgfs.{select_date}%2F{release}%2Fatmos&' \
                  f'file=gfs.t{release}z.pgrb2.0p25.f{hour_key}&all_var=on&all_lev=on&subregion=&toplat=53&leftlon=22&rightlon=42&bottomlat=44'

            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

            with urllib.request.urlopen(url, context=context) as response:
                data = response.read()

                content_disposition = response.headers.get('Content-Disposition')
                file_name = content_disposition.split("filename=")[-1].strip("'\"")

                destination_path = os.path.join(destination_folder, file_name)
                with open(destination_path, 'wb') as out_file:
                    out_file.write(data)
                print(f"Файл успішно завантажено в {destination_path}")
        except Exception as e:
            print(f"Помилка під час завантаження файлу з URL: {url}, {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=120) as executor:
        futures = []
        for hour_key in type_keys:
            futures.append(executor.submit(download_file, hour_key))
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Помилка під час завантаження: {e}")

#
download_files(three_hour_keys, select_date, "00")
# download_files(one_hour_keys, select_date, "06")
# download_files(three_hour_keys, select_date, "12")
# download_files(one_hour_keys, select_date, "18")
