"""
Name:    extraction.py
Purpose: Extract V1 GDELT Records and store them on disk in a specific structure.
"""

from schema import v1_base, v1_adds
from bs4 import BeautifulSoup
import requests
import zipfile
import pandas
import time
import os


def get_directory(base_dir, name):

    target = os.path.join(base_dir, name)

    if not os.path.exists(target):
        os.makedirs(target)

    return target


def fetch_year_urls(year):

    root_event_url = 'http://data.gdeltproject.org/events'

    response = requests.get(f'{root_event_url}/index.html')
    the_soup = BeautifulSoup(response.content, features='lxml')

    url_list = [f'{root_event_url}/{i["href"]}' for i in the_soup.find_all('a') if i['href'].startswith(str(year))]

    return url_list


def extract_data(csv_url, temp_dir):

    print(f'Collecting GDELT Record: {csv_url}')

    response = requests.get(csv_url, stream=True)

    zip_name = csv_url.split('/')[-1]
    zip_path = os.path.join(temp_dir, zip_name)

    with open(zip_path, 'wb') as file: file.write(response.content)
    with zipfile.ZipFile(zip_path, 'r') as the_zip: the_zip.extractall(temp_dir)

    txt_name = zip_name.strip('export.CSV.zip')
    txt_name += '.txt'
    txt_path = os.path.join(temp_dir, txt_name)

    os.rename(zip_path.strip('.zip'), txt_path)

    return txt_path


if __name__ == "__main__":

    start_time = time.time()

    this_dir = os.path.split(os.path.realpath(__file__))[0]

    data_dir = get_directory(this_dir, 'gdelt_events')
    year_dir = get_directory(data_dir, 'gdelt_2018')

    all_urls = fetch_year_urls(2018)

    existing = [os.path.basename(i).split('.')[0] for i in os.listdir(year_dir)]

    for url in all_urls:

        txt_path = extract_data(url, year_dir)
        gdelt_df = pandas.read_csv(txt_path, sep='\t', names=list(v1_base.keys()) + v1_adds, dtype=v1_base)
        gdelt_df.dropna(subset=['ActionGeo_Long', 'ActionGeo_Lat'], inplace=True)
        gdelt_df.drop_duplicates('SOURCEURL', inplace=True)
        csv_path = os.path.join(year_dir, f"{os.path.basename(txt_path).split('.')[0]}.csv")

        gdelt_df.to_csv(csv_path, index=False)
        os.remove(txt_path)

    print(f'Extracted GDELT Data in: {round((time.time() - start_time) / 60, 2)} minute(s)')
