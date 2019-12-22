import drive_connector
import logging
import os
import postgres_connector
import gzip
import pandas as pd



def send_files(settings):

    server_files = drive_connector.list_files(settings)
    server_file_names = []
    for server_file in server_files:
        server_file_names.append(server_file['originalFilename'])

    local_files = os.listdir("/Users/luc/Desktop/scrapped_data/")
    for local_file in local_files:
        if local_file not in server_file_names:
            with open(f"/Users/luc/Desktop/scrapped_data/{local_file}","r") as file:
                drive_connector.upload_file(file, settings)
            logging.info(f"{local_file} was uploaded, juhu.")


def import_server_files_to_postgres(settings):
    server_files = drive_connector.list_files(settings)
    temp_fn = 'temp_file.csv.gz'
    i = 0
    for server_file in server_files:
        if server_file['originalFilename'] > '2019-12-01' and server_file['originalFilename'][-28:] == "scooter_position_logs.csv.gz" :
            logging.info(server_file['originalFilename'] + " | " + str(i) + "/" + str(len(server_files)))
            server_file.GetContentFile(temp_fn) # Download file as 'catlove.png'.
            df = pd.read_csv(gzip.open(temp_fn))
            spls = df.to_dict(orient='records')
            spl_example = spls[0]
            already_in_db = postgres_connector.check_spl_unicity(settings, spl_example)
            if not already_in_db:
                postgres_connector.save_to_postgres(spls, settings)
                logging.info(f"saved {len(spls)} spls to DB")
            else:
                logging.info(f"skipped, already in DB")
        i += 1
