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
from texttable import Texttable


class City:
    def __init__(self, city_dict, config):
        self.name = city_dict['name']
        self.lat = city_dict['lat']
        self.lng = city_dict['lng']
        self.sw_lat = city_dict['sw_lat']
        self.sw_lng = city_dict['sw_lng']
        self.ne_lat = city_dict['ne_lat']
        self.ne_lng = city_dict['ne_lng']
        self.tier_city_name = city_dict['tier_city_name']
        self.providers = []

        for provider in list(Providers):
            if provider.name in city_dict and city_dict[provider.name] == 1:
                self.providers.append(provider)


def get_cities(config):
    url = config["PROVIDERS"]['cities_providers_url']
    df = pd.read_csv(url)
    cities = []
    for index, city_dict in df.iterrows():
        cities.append(City(city_dict, config))
    return cities


def init_logger(config):
    logging.basicConfig(level=logging.INFO,handlers=[
        logging.FileHandler(filename='scooter_scrapper.log', mode='a'),
        logging.StreamHandler()
    ], format="%(asctime)s;%(levelname)s;%(message)s")


if __name__ == '__main__':
    if len(sys.argv) > 1:
        os.chdir(sys.argv[1])

    settings = ConfigParser()
    settings.read('settings.ini')
    init_logger(settings)

    cities = get_cities(settings)
    all_spls = []
    table_headers = ['City']


    for provider in list(Providers):
        if provider.value.frontend:
            all_spls.extend(provider.value.get_scooters())
            logging.info(f"{len(all_spls)} scooters retrieved from {provider.name}")
        else:
            table_headers.append(provider.name)

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

                if len(spls) > 0:
                    city_log[provider.name] = len(spls)
                else:
                    city_log[provider.name] = '❌'
                    error = True

            except (Exception) as error:
                logging.error(f"problem by retrieving scooters from {provider.name}, error: {error}")
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

    if len(error_rows) > 1:
        error_table = Texttable()
        error_table.set_cols_align(["l", "c", "c", "c", "c", "c"])
        error_table.add_rows(error_rows)
        logging.info("\n" + error_table.draw())
        # sending the table to slack
        if settings['SLACK']:
            slack_connector.post_message(settings, "```" + table.draw() + "```")



    # Saving the data as a zipped CSV
    df = DataFrame(all_spls)
    df = DataFrame.from_records([s.to_dict() for s in all_spls])
    ts = datetime.now().isoformat(sep=' ',timespec='seconds')
    df.to_csv (f'scrapped_data/{ts}_scooter_position_logs.csv.gz', header=True, index=False, compression='gzip')

    # Saving the data to Postgres
    if settings['POSTGRES']:
        postgres_connector.save_to_postgres(all_spls, settings)


