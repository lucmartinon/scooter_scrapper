import logging, logging.handlers
from configparser import ConfigParser
import providers.circ
import providers.lime
import providers.voi
import providers.tier
import providers.bird
from providers.provider import Provider, ScooterPositionLog
from typing import Iterable
from pandas import DataFrame
from datetime import datetime
import psycopg2
import sys
import os

class City():
    def __init__(self, name: str, lat: float, lng: float, providers: Iterable[Provider], tier_city_name, sw_lat, sw_lng, ne_lat, ne_lng):
        self.name = name
        self.lat = lat
        self.lng = lng
        self.providers = providers
        self.sw_lat = sw_lat
        self.sw_lng = sw_lng
        self.ne_lat = ne_lat
        self.ne_lng = ne_lng
        self.tier_city_name = tier_city_name


def init_logger(config):
    # email_handler = logging.handlers.SMTPHandler(
    #     mailhost=('smtp.gmail.com', 587),
    #     fromaddr="af.supp.de@gmail.com",
    #     toaddrs=config['POSTGRES']['email_error'],
    #     subject=u"scooter scrapper error!",
    #     credentials=("af.supp.de@gmail.com", ""))
    # email_handler.setLevel(logging.ERROR)

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

    cities = [
        City(name="Berlin",
             lat=52.4864452,
             lng=13.4003343,
             providers=[
                providers.circ.Circ(config),
                providers.voi.Voi(config),
                providers.tier.Tier(config),
                providers.bird.Bird(config),
                providers.lime.Lime(config)
             ],
             tier_city_name="BERLIN",
             ne_lat=52.55225,
             ne_lng=13.49395,
             sw_lat=52.46198,
             sw_lng=13.27010
             )
    ]

    all_spls = []
    for city in cities:
        for provider in city.providers:
            try:
                spls = provider.get_scooters(city)
                logging.info(f"{len(spls)} scooters retrieved from {provider.provider}")
                all_spls.extend(spls)
            except (Exception) as error:
                logging.error(f"problem by retrieving scooters from {provider.provider}, error: {error}")

    df = DataFrame(all_spls)

    df = DataFrame.from_records([s.to_dict() for s in all_spls])
    ts = datetime.now().isoformat(sep=' ',timespec='seconds')
    df.to_csv (f'scrapped_data/{ts}_scooter_position_logs.csv.gz', header=True, index=False, compression='gzip')

    save_to_postgres(all_spls, config)











