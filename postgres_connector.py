import psycopg2
import logging
import slack_connector
import traceback
import gzip

conn = None;


def get_conn(settings):
    global conn
    if conn is None:
        # read connection parameters
        params = settings['POSTGRES']
        # connect to the PostgreSQL server
        conn = psycopg2.connect(host=params['host'], database=params['database'], user=params['user'],
                                password=params['password'])
        logging.info('connected to db')
    return conn;


def get_cursor(settings):
    return get_conn(settings).cursor()


def refresh_views(settings):
    run_query(settings, """
insert into scooters  (
    select distinct
        city, provider, scooter_id,
        first_value(timestamp) over win scooter_first_seen,
        last_value(timestamp) over win scooter_last_seen,
        last_value(lng) over win last_lng,
        last_value(lat) over win last_lat
    from new_spl
    where provider <> 'lime' and city <> ''
    WINDOW win as (partition by city, provider, scooter_id order by timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )
    )
on conflict on constraint scooters_pkey
do update 
	set (scooter_last_seen, last_lng, last_lat) = (excluded.scooter_last_seen, excluded.last_lng, excluded.last_lat)
    """)
    run_query(settings, "refresh materialized view city_providers;")
    run_query(settings, "refresh materialized view mass_extinctions;")


def run_query(settings, query):
    conn = get_conn(settings)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()


def run_query_on_stat_server(settings, query):
    params = settings['STAT_SERVER']
    stat_conn = psycopg2.connect(host=params['server'], database=params['database'], user=params['db_user'], password=params['db_password'])
    cur = stat_conn.cursor()
    cur.execute(query)
    stat_conn.commit()
    cur.close()


def get_max_scooter_ts_in_db(settings):
    cur = get_cursor(settings)
    sql = "select max(timestamp) from scooter_position_logs"
    cur.execute(sql)
    result = str(cur.fetchall()[0][0])
    cur.close()
    return result


def check_spl_unicity(settings, spl):
    cur = get_cursor(settings)
    sql = "select 1 from scooter_position_logs where city = %(city)s and provider = %(provider)s and scooter_id = %(id)s and timestamp = %(timestamp)s"
    cur.execute(sql, spl)
    results = cur.fetchall()
    cur.close()
    return len(results) > 0


def delete_new_spl(settings):
    run_query(settings, "delete from new_spl")


def load_spl_csv_to_postgres(settings, dir, file):
    cur = get_cursor(settings)
    with gzip.open(dir + file, 'r') as f:
        next(f)  # Skip the header row.
        cur.copy_from(f, 'scooter_position_logs', sep=',')
        cur.copy_from(f, 'new_spl', sep=',')
        logging.info(f"{file[0:10]}: saved {cur.rowcount} spls to DB")
        conn.commit()
    cur.close()



def save_to_postgres(spls, settings):
    conn = None
    try:
        # read connection parameters
        params = settings['POSTGRES']

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
            if getattr(spl, "to_dict", None) is not None:
                dict = spl.to_dict()
            else:
                dict = spl
            cur.execute(sql, dict)

        conn.commit()

        logging.info(f"{len(spls)} scooters positions logs saved to postgres DB")

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        msg = f"\n❌❌❌❌❌\nUnexpected problem prevented scooter_scrapper termination: ```{traceback.format_exc()}```\n❌❌❌❌❌"
        logging.error(msg)
        slack_connector.post_message(settings, msg)
    finally:
        if conn:
            conn.close()

