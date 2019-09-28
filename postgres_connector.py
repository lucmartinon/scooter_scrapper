import psycopg2
import logging


def save_to_postgres(spls, config):
    conn = None
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
        if conn:
            conn.close()
