import logging, logging.handlers
from configparser import ConfigParser
import providers.circ
import providers.lime
import providers.voi
import providers.tier
import providers.bird
from pandas import DataFrame
from datetime import datetime
import psycopg2
import sys
import os
import pandas as pd


class City():
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
        if city_dict['bird'] == 1:
            self.providers.append(providers.bird.Bird(config))
        if city_dict['tier'] == 1:
            self.providers.append(providers.tier.Tier(config))
        if city_dict['lime'] == 1:
            self.providers.append(providers.lime.Lime(config))
        if city_dict['circ'] == 1:
            self.providers.append(providers.circ.Circ(config))
        if city_dict['voi'] == 1:
            self.providers.append(providers.voi.Voi(config))



def get_cities(config):
    url = config['DEFAULT']['cities_providers_url']
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


def save_to_postgres(spls, config):

    try:
        # read connection parameters
        params = config['POSTGRES']

        # connect to the PostgreSQL server
        conn = psycopg2.connect(host=params['host'], database=params['database'], user=params['user'], password=params['password'])

        # create a cursor
        cur = conn.cursor()
        sql = """
        INSERT INTO scooter_position_logs (city, provider, id, secondary_id, location, timestamp, battery_level, licence_plate, raw_data)
             VALUES (%(city)s, %(provider)s, %(id)s, %(secondary_id)s, point(%(lat)s, %(lng)s), %(timestamp)s, %(battery_level)s, %(licence_plate)s, %(raw_data)s);
        """
        # execute a statement
        for spl in spls:
            cur.execute(sql, spl.to_dict())

        conn.commit()

        logging.info(f"{len(spls)} scooters positions logs saved to postgres DB")

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        os.chdir(sys.argv[1])

    config = ConfigParser()
    config.read('settings.ini')
    init_logger(config)

    cities = get_cities(config)
    all_spls = []
    for city in cities:
        for provider in city.providers:
            try:
                spls = provider.get_scooters(city)
                logging.info(f"{city.name}: {len(spls)} scooters retrieved from {provider.provider}")
                all_spls.extend(spls)
            except (Exception) as error:
                logging.error(f"problem by retrieving scooters from {provider.provider}, error: {error}")

    df = DataFrame(all_spls)

    df = DataFrame.from_records([s.to_dict() for s in all_spls])
    ts = datetime.now().isoformat(sep=' ',timespec='seconds')
    df.to_csv (f'scrapped_data/{ts}_scooter_position_logs.csv.gz', header=True, index=False, compression='gzip')

    save_to_postgres(all_spls, config)
