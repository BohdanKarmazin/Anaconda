import multiprocessing
import time
import aiohttp
import asyncio
import os
import bz2
import schedule
from bs4 import BeautifulSoup
import re


async def parser_file(parameter, release, max_files=25):
    url = f"https://opendata.dwd.de/weather/nwp/icon-eu/grib/{release}/{parameter}/"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                soup = BeautifulSoup(await response.text(), 'html.parser')
                links = soup.find_all('a')
                pattern = re.compile(r'icon-eu_europe_regular-lat-lon_.*\.grib2\.bz2')
                downloaded_files = 0

                # Директорії для збереження файлів
                output_dir = f'D:\\ANACONDA\\{parameter}\\Parse_ICON_Grib_bz\\'
                output_dir2 = f'D:\\ANACONDA\\{parameter}\\Parse_ICON_Grib\\'
                file_path = f'D:\\ANACONDA\\{parameter}\\Parse_ICON_Grib_bz'
                file_path1 = f'D:\\ANACONDA\\{parameter}\\Parse_ICON_Grib'

                try:
                    files = os.listdir(file_path)
                    for file_name in files:
                        file = os.path.join(file_path, file_name)
                        if os.path.isfile(file):
                            os.remove(file)
                    print(f'Усі файли в папці {file_path} були видалені.')

                    files = os.listdir(file_path1)
                    for file_name in files:
                        file = os.path.join(file_path1, file_name)
                        if os.path.isfile(file):
                            os.remove(file)
                    print(f'Усі файли в папці {file_path1} були видалені.')

                except Exception as e:
                    print(f'Сталася помилка: {e}')

                for link in links:
                    href = link.get('href')
                    if href and pattern.search(href):
                        print(href)
                        file_url = f"{url}{href}"

                        file_response = await session.get(file_url)
                        if file_response.status == 200:
                            # Створення файлу та запис даних асинхронно
                            file_content = await file_response.read()
                            with open(f"{file_path}/{href}", "wb") as file:
                                file.write(file_content)
                            print(f"{file_url}: завантажений успішно.")
                            downloaded_files += 1
                            if downloaded_files >= max_files:
                                break
                        else:
                            print(f"Помилка при завантаженні файлу {href}. Код статусу:", file_response.status)
                    else:
                        print('скоріше за все регулярний вираз для силки не підходить')
            else:
                print("Помилка при зверненні до сторінки. Код статусу:", response.status)

    files = os.listdir(output_dir)
    for file in files:
        compressed_file_path = f'{output_dir}\\{file}'
        output_file_path = f'{output_dir2}\\{str(file)[:-4]}'
        try:
            with bz2.open(compressed_file_path, 'rb') as compressed_file:
                with open(output_file_path, 'wb') as output_file:
                    for data in iter(lambda: compressed_file.read(100 * 1024), b''):
                        output_file.write(data)
            print(f"Файл розархівовано до {output_file_path}")
        except Exception as e:
            print(f"Помилка: {str(e)}")


async def run(release):
    parameters = ['cape_con']
    tasks = [parser_file(par, release, 30) for par in parameters]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    multiprocessing.freeze_support()

    async def main():
        await run("09")


    asyncio.run(main())