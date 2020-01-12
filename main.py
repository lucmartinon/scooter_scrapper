import logging, logging.handlers
from configparser import ConfigParser
from providers.providers import Providers
from pandas import DataFrame
from datetime import datetime
import sys
import os
import pandas as pd
import slack_connector
import postgres_connector
import drive_connector
import traceback
from texttable import Texttable


class City:
    def __init__(self, city_dict):
        self.name = city_dict['name']
        self.lat = city_dict['lat']
        self.lng = city_dict['lng']
        self.sw_lat = city_dict['sw_lat']
        self.sw_lng = city_dict['sw_lng']
        self.ne_lat = city_dict['ne_lat']
        self.ne_lng = city_dict['ne_lng']
        self.tier_city_name = city_dict['tier_city_name']
        self.voi_zone_id = city_dict['voi_zone_id']
        self.providers = []

        for provider in list(Providers):
            if provider.name in city_dict and city_dict[provider.name] == 1:
                self.providers.append(provider)


def get_cities(config):
    url = config["PROVIDERS"]['cities_providers_url']
    df = pd.read_csv(url)
    cities = []
    for index, city_dict in df.iterrows():
        cities.append(City(city_dict))
    return cities


def init_logger():

    logging.basicConfig(level=logging.INFO,handlers=[
        logging.FileHandler(filename='scooter_scrapper.log', mode='a'),
        logging.StreamHandler()
    ], format="%(asctime)s;%(levelname)s;%(message)s")
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)


def scrap_scooters(settings):
    cities = get_cities(settings)
    all_spls = []
    table_headers = ['City']
    requests = 0

    for provider in list(Providers):
        try:
            if provider.value.frontend:
                all_spls.extend(provider.value.get_scooters())
                logging.info(f"{len(all_spls)} scooters retrieved from {provider.name}")
                requests += 1
            else:
                table_headers.append(provider.name)

        except Exception as err:
            logging.error(f"problem by retrieving scooters from {provider.name}, error: {err}")

    log_rows = [table_headers]
    error_rows = [table_headers]

    for city in cities:
        city_log = {}
        error = False

        for provider in list(Providers):
            if not provider.value.frontend:
                city_log[provider.value.name] = ''

        for provider in city.providers:
            try:
                spls = provider.value.get_scooters(settings, city)
                logging.info(f"{city.name}: {len(spls)} scooters retrieved from {provider.name}")
                all_spls.extend(spls)
                requests += 1

                if len(spls) > 0:
                    city_log[provider.name] = len(spls)
                else:
                    city_log[provider.name] = '❌'
                    error = True

            except Exception as err:
                logging.error(f"problem by retrieving scooters from {provider.name}, error: {err}")
                city_log[provider.name] = '❌'
                error = True

            log_row = [city.name]
            for provider in list(Providers):
                if not provider.value.frontend:
                    log_row.append(city_log[provider.name])

        log_rows.append(log_row)
        if error:
            error_rows.append(log_row)

    # Making a nice table summarizing results
    table = Texttable()
    table.set_cols_align(["l", "c", "c", "c", "c", "c"])
    table.add_rows(log_rows)
    logging.info("\n" + table.draw())

    ts = datetime.now().isoformat(sep=' ',timespec='seconds')
    summary = f"Scooter Scrapper Result, run finish: {ts}\n\t{len(all_spls)} scooter position scrapped\n\t{requests} successful requests"

    if len(error_rows) > 1:
        error_table = Texttable()
        error_table.set_cols_align(["l", "c", "c", "c", "c", "c"])
        error_table.add_rows(error_rows)
        summary += f"\n\t{len(error_rows) -1} cities with errors: \n```\n" + error_table.draw() + "\n```"

    logging.info(summary)

    if 'SLACK' in settings:
        slack_connector.post_message(settings, summary)

    # Saving the data as a zipped CSV
    df = DataFrame.from_records([s.to_dict() for s in all_spls])

    df.to_csv(f'scrapped_data/{ts}_scooter_position_logs.csv.gz', header=True, index=False, compression='gzip')

    if 'GOOGLE_DRIVE' in settings:
        with open(f'scrapped_data/{ts}_scooter_position_logs.csv.gz',"r") as file:
            drive_connector.upload_file(file, settings)

    logging.info(f"File {ts}_scooter_position_logs.csv.gz correctly uploaded to Google Drive.")


def get_settings():
    settings = ConfigParser()
    settings.read('settings.ini')
    return settings


if __name__ == '__main__':
    if len(sys.argv) > 1:
        os.chdir(sys.argv[1])

    init_logger()
    settings = get_settings()

    try:
        scrap_scooters(settings)
    except Exception as err:
        msg = f"\n❌❌❌❌❌\nUnexpected problem prevented scooter_scrapper termination: ```{traceback.format_exc()}```\n❌❌❌❌❌"
        logging.error(msg)
        slack_connector.post_message(settings, msg)



