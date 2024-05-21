import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

hours_pars = ['0h', '3h', '6h', '9h', '12h', '15h', '18h', '21h', '24h', '27h', '30h', '33h', '36h', '39h', '42h', '45h', '48h', '51h']

url = "https://data.ecmwf.int/forecasts/20240516/00z/ifs/0p25/oper/"

file_extension = ".grib2"

file_path = "D:\\ANACONDA\\ECMWF"


files = os.listdir(file_path)
for file_name in files:
    file = os.path.join(file_path, file_name)
    if os.path.isfile(file):
        os.remove(file)
print(f'Усі файли в папці {file_path} були видалені.')

response = requests.get(url)
if response.status_code == 200:
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    soup = BeautifulSoup(response.content, 'html.parser')

    links = soup.find_all('a')

    for link in links:
        href = link.get('href')
        if href and href.endswith(file_extension):
            file_url = urljoin(url, href)   # Створити повне посилання
            filename = os.path.basename(file_url)
            split_hour = filename.split('-')[1]
            for hour_pars in hours_pars:
                if hour_pars == split_hour:
                    with open(os.path.join(file_path, filename), 'wb') as file:
                        file.write(requests.get(file_url).content)

                        print(f"Завантажено: {filename}")
else:
    print("Не вдалося отримати доступ до веб-сторінки.")




