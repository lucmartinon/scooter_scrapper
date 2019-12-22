from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials
import os


def get_drive(settings):
    gauth = GoogleAuth()
    scope = ['https://www.googleapis.com/auth/drive']

    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(settings["GOOGLE_DRIVE"]["filename"], scope)
    drive = GoogleDrive(gauth)
    return drive


def list_files(settings):
    drive = get_drive(settings)
    folder_id = settings["GOOGLE_DRIVE"]["folder_id"]
    file_list = drive.ListFile({'q': f"\'{folder_id}\' in parents and trashed=false"}).GetList()
    return file_list



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

