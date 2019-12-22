import psycopg2
import logging

conn = None;


def get_cursor(config):
    global conn
    if conn is None:
        # read connection parameters
        params = config['POSTGRES']

        # connect to the PostgreSQL server
        conn = psycopg2.connect(host=params['host'], database=params['database'], user=params['user'], password=params['password'])
        logging.info('connected to db')

    return conn.cursor()


def check_spl_unicity(config, spl):
    cur = get_cursor(config)
    sql = "select 1 from scooter_position_logs where city = %(city)s and provider = %(provider)s and scooter_id = %(id)s and timestamp = %(timestamp)s"
    cur.execute(sql, spl)
    results = cur.fetchall()
    return len(results) > 0


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
        INSERT INTO scooter_position_logs (city, provider, scooter_id, secondary_id, location, timestamp, battery_level, licence_plate)
             VALUES (%(city)s, %(provider)s, %(id)s, %(secondary_id)s, point(%(lng)s, %(lat)s), %(timestamp)s, %(battery_level)s, %(licence_plate)s);
        """
        # execute a statement
        for spl in spls:
            dict = None
            if "to_dict" in spl:
                dict = spl.to_dict()
            else:
                dict = spl
            cur.execute(sql, dict)

        conn.commit()

        logging.info(f"{len(spls)} scooters positions logs saved to postgres DB")

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        if conn:
            conn.close()
