import drive_connector
import logging
import os
import postgres_connector
import gzip
import pandas as pd
import main
from datetime import date
import paramiko
import time
import math

dtype = {
    "battery_level": "int64",
    "city": "object",
    "id":"object",
    "lat": "float64",
    "licence_plate":"object",
    "lng": "float64",
    "provider":"object",
    "secondary_id": "object",
    "timestamp": "object"
}


def upload_all_local_files(settings):
    server_files = drive_connector.list_drive_files(settings)
    server_file_names = []
    for server_file in server_files:
        server_file_names.append(server_file['originalFilename'])

    local_files = os.listdir("/Users/luc/Desktop/scrapped_data/")
    for local_file in local_files:
        if local_file not in server_file_names:
            with open(f"/Users/luc/Desktop/scrapped_data/{local_file}","r") as file:
                drive_connector.upload_file(file, settings)
            logging.info(f"{local_file} was uploaded, juhu.")


def load_daily_files_to_postgres(settings, dir, last_day_loaded):
    postgres_connector.delete_new_spl(settings)
    files = sorted(os.listdir(dir))
    global dtype
    i = 0
    for file in files:
        if file.endswith("_spls.csv.gz") and file[0:10] > last_day_loaded:
            postgres_connector.load_spl_csv_to_postgres(settings, dir, file)



def merge_csv_files_per_day(in_dir, out_dir):
    raw_files = sorted(os.listdir(in_dir))
    existing_daily_files = sorted(os.listdir(out_dir))
    current_day = None
    current_pd = None
    i = 0
    j = 0
    file_cnt = len(raw_files)
    for raw_file in raw_files:
        if raw_file.endswith("_scooter_position_logs.csv.gz") and raw_file[0:10] < str(date.today()) and f"{raw_file[0:10]}_spls.csv.gz" not in existing_daily_files \
                and not raw_file[0:13] == "2020-02-21 05":

            if current_day is None:
                current_day = raw_file[0:10]
            file_day = raw_file[0:10]

            if file_day != current_day: #filesdifferent day, we write the file and set current_pd to None
                del current_pd["raw_data"]
                current_pd.to_csv(f'{out_dir}{current_day}_spls.csv.gz', header=True, index=False, compression='gzip')
                current_day = file_day
                current_pd = None
                j += 1

            if current_pd is None: ## start or new daz
                current_pd = pd.read_csv(gzip.open(in_dir + raw_file))
            else:
                current_pd = current_pd.append(pd.read_csv(in_dir + raw_file))
            logging.debug(f'Processed file {i}/{file_cnt}: {raw_file}')
            i += 1

    #writing last csv if needed
    if current_pd is not None:
        del current_pd["raw_data"]
        current_pd.to_csv(f'{out_dir}{current_day}_spls.csv.gz', header=True, index=False, compression='gzip')
        j += 1

    logging.info(f'Reviewed {i} hourly files, created {j} new daily file(s)')
    return j


def send_stat_files_to_server(settings):

    if 'STAT_SERVER' in settings:

        local_path = "/tmp/scooter_summary.csv"
        remote_path = "/tmp/scooter_summary.csv"

        postgres_connector.run_query(settings, f"COPY (select * from scooter_summary) TO '{local_path}' CSV HEADER")

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        k = paramiko.RSAKey.from_private_key_file(settings['SSH']['file'])
        ssh.connect(settings['STAT_SERVER']['server'], username=settings['STAT_SERVER']['user'], pkey=k)
        sftp = ssh.open_sftp()
        sftp.put(local_path, remote_path)
        sftp.close()
        ssh.close()
        logging.info(f"scooter summary uploaded to remote server")
        postgres_connector.run_query_on_stat_server(settings, f"delete from scooter_summary; copy scooter_summary from '{local_path}' csv header;")


def refresh_stat_views(settings):
    if 'STAT_SERVER' in settings:
        postgres_connector.run_query_on_stat_server(settings, "refresh materialized view mass_extinctions;")


def import_new_data(settings):
    raw_dir = "downloaded_data/raw/"
    daily_dir = "downloaded_data/daily/"

    # download all new files
    drive_connector.download_new_files(settings, raw_dir, "2020-01-01")

    # merge hourly csv files into daily files, removing column "raw_data", only for non existing date before today  (to have only complete days)
    days_loaded = merge_csv_files_per_day(raw_dir, daily_dir)

    # load daily file to DB if they are newer than last data in DB
    last_data_in_db = postgres_connector.get_max_scooter_ts_in_db(settings)
    last_day_loaded = str(last_data_in_db)[0:10]
    logging.info(f"last data in DB: {last_day_loaded}")
    load_daily_files_to_postgres(settings, daily_dir, last_day_loaded)
    return days_loaded


def diff_time(start_time):
    diff_sec =  time.time() - start_time
    hour, min, sec = "", "", ""
    if diff_sec > 3600:
        hour = f"{math.floor(diff_sec / 3600):02}:"
    if diff_sec > 60:
        min = f"{math.floor(diff_sec % 3600 / 60):02}:"
    sec = f"{round(diff_sec % 3600 % 60):02}"
    return hour + min + sec




if __name__ == '__main__':

    start_time = time.time()
    main.init_logger()
    settings = main.get_settings()
    logging.info("start local process")
    days_loaded = import_new_data(settings)

    if days_loaded >= 0:
        view_start_time = time.time()
        postgres_connector.refresh_views(settings)
        logging.info(f"wiews refreshed in {diff_time(view_start_time)}")
        send_stat_files_to_server(settings)
        refresh_stat_views(settings)

    sec = time.time() - start_time
    logging.info(f"end local process after {diff_time(start_time)} - stat DB is up to date until yesterday")