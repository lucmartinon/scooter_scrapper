import drive_connector
import logging
import os
import postgres_connector
import gzip
import pandas as pd
import main
from datetime import date

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
    files = sorted(os.listdir(dir))
    global dtype
    i = 0
    for file in files:
        if file.endswith("_spls.csv.gz") and file[0:10] > last_day_loaded:
            df = pd.read_csv(gzip.open(dir + file), dtype=dtype)
            spls = df.to_dict(orient='records')
            postgres_connector.save_to_postgres(spls, settings)
            logging.info(f"{file[0:10]}: saved {len(spls)} spls to DB")


def merge_csv_files_per_day(in_dir, out_dir):
    raw_files = sorted(os.listdir(in_dir))
    existing_daily_files = sorted(os.listdir(out_dir))
    current_day = None
    current_pd = None
    i = 0
    file_cnt = len(raw_files)
    for raw_file in raw_files:
        if raw_file.endswith("_scooter_position_logs.csv.gz") and raw_file[0:10] < str(date.today()) and f"{raw_file[0:10]}_spls.csv.gz" not in existing_daily_files:
            if current_day is None:
                current_day = raw_file[0:10]
            file_day = raw_file[0:10]

            if file_day != current_day: #filesdifferent day, we write the file and set current_pd to None
                del current_pd["raw_data"]
                current_pd.to_csv(f'{out_dir}{current_day}_spls.csv.gz', header=True, index=False, compression='gzip')
                current_day = file_day
                current_pd = None

            if current_pd is None: ## start or new daz
                current_pd = pd.read_csv(gzip.open(in_dir + raw_file))
            else:
                current_pd = current_pd.append(pd.read_csv(in_dir + raw_file))
            logging.info(f'Processed file {i}/{file_cnt}: {raw_file}')
            i += 1

    #writing last csv if needed
    if current_pd is not None:
        del current_pd["raw_data"]
        current_pd.to_csv(f'{out_dir}{current_day}_spls.csv.gz', header=True, index=False, compression='gzip')


def import_new_data(settings):
    raw_dir = "downloaded_data/raw/"
    daily_dir = "downloaded_data/daily/"

    # download all new files
    drive_connector.download_new_files(settings, raw_dir, "2020-01-01")

    # merge hourly csv files into daily files, removing column "raw_data", only for non existing date before today  (to have only complete days)
    merge_csv_files_per_day(raw_dir, daily_dir)

    # load daily file to DB if they are newer than last data in DB
    last_data_in_db = postgres_connector.get_max_scooter_ts_in_db(settings)
    last_day_loaded = str(last_data_in_db)[0:10]
    logging.info(f"last data in DB: {last_day_loaded}")
    load_daily_files_to_postgres(settings, daily_dir, last_day_loaded)


if __name__ == '__main__':
    main.init_logger()
    settings = main.get_settings()
    import_new_data(settings)