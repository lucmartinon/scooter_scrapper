from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
import logging
import os


def get_drive(settings):
    gauth = GoogleAuth()
    scope = ['https://www.googleapis.com/auth/drive']

    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(settings["GOOGLE_DRIVE"]["filename"], scope)
    drive = GoogleDrive(gauth)
    return drive


def list_drive_files(settings):
    drive = get_drive(settings)
    folder_id = settings["GOOGLE_DRIVE"]["folder_id"]
    file_list = drive.ListFile({'q': f"\'{folder_id}\' in parents and trashed=false"}).GetList()
    return file_list


def download_new_files(settings, dir, min_date):
    local_files = sorted(os.listdir(dir))
    drive_files = list_drive_files(settings)
    i = 0
    for file in drive_files:
        if file['originalFilename'].endswith("_scooter_position_logs.csv.gz") and file['originalFilename'] not in local_files and file['originalFilename'] > min_date:
            file.GetContentFile(dir + file['originalFilename'])
            logging.info('downloaded file ' + file['originalFilename'])
            i += 1
        else:
            logging.debug(f"skipping {file['originalFilename']}, either not a spl file, or already there, or before min_date")
    logging.info(f'downloaded {i} new raw files into {dir}')


def upload_file(file, settings):
    drive = get_drive(settings)
    file_drive = drive.CreateFile({
        'title':os.path.basename(file.name),
        'parents': [{
            "kind": "drive#childList",
            "id": settings["GOOGLE_DRIVE"]["folder_id"]
        }]
    })
    file_drive.SetContentFile(file.name)
    file_drive.Upload()
    file_drive.InsertPermission({
      'value': settings["GOOGLE_DRIVE"]["email_user"],
      'type': "user",
      'role': "writer"
    })
    file_drive.Upload()


def create_folder(settings):
    drive = get_drive(settings)
    folder_metadata = {'id': '1m5MbB_vXbGJAm-9vEuDw2yRWGr3MczrA', 'title' : 'scrapped_data', 'mimeType' : 'application/vnd.google-apps.folder'}
    new_permission = {
      'value': 'luc.martinon@gmail.com',
      'type': "user",
      'role': "owner"
    }

    folder = drive.CreateFile(folder_metadata)
    folder.Upload()


def delete_file(file_id, settings):
    drive = get_drive(settings)
    file_drive = drive.CreateFile({'id': file_id})
    file_drive.Delete()

