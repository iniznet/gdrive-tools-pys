from __future__ import print_function
import httplib2
import os
import json
import pickle
import sys

from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools
from oauth2client import file
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import signal
from multiprocessing import Pool
from functools import partial

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

SCOPES = ['https://www.googleapis.com/auth/drive']
credentials_json = 'oauth-credentials.json'
credentials_pickle = 'token.pickle'

def get_credentials():
    creds = None
    # Obtain OAuth token / user authorization.
    if os.path.exists(credentials_pickle):
        with open(credentials_pickle, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_json, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(credentials_pickle, 'wb') as token:
            pickle.dump(creds, token)
    return creds

def rename_file(service, file_id, judul_baru, judul_lama):
    try:
        file = {
            'name': judul_baru
            }
        updated_file = service.files().update(supportsAllDrives=True,
                supportsTeamDrives=True,
                fileId=file_id,
                body=file).execute()
        print('Berhasil merename file '+ judul_lama + ' ke ' + judul_baru)
        return updated_file
    except:
        print('Terjadi kesalahan saat sedang merename file ' + file['name'] + ' (' + file['id'] + ')')
        return None

def list_files_in_folder(service, folder_id):
    files = []
    page_token = None

    query = "'" + folder_id + "' in parents"
    response1 = service.files().list(supportsAllDrives=True, includeItemsFromAllDrives=True, q=query, fields='files(id, name, parents)').execute()
    query = "'" + response1['files'][0]['id'] + "' in parents"
    response2 = service.files().list(supportsAllDrives=True, includeItemsFromAllDrives=True, q=query, fields='files(id, name, parents, fileExtension)', pageSize=1000).execute()

    for folder in response2['files']:
        try:
            fileType = folder['fileExtension']
        except:
            while True:
                try:
                    param = {}
                    if page_token:
                        param['pageToken'] = page_token
                    print('Mendapatkan semua file di dalam folder ' + folder['name'] + ' (' + folder['id'] + ')')
                    children = service.files().list(supportsAllDrives=True, includeItemsFromAllDrives=True, q="'"+ folder['id'] +"' in parents and name contains 'Copy of'", fields='files(id)', pageSize=1000, **param).execute()
                    for child in children.get('files', []):
                        files.append(child['id'])
                    page_token = children.get('nextPageToken')
                    if not page_token:
                        break
                except:
                    print('Terjadi kesalahan saat list mendapatkan semua file di folder ' + folder['name'] + '(' + folder['id'] + ')')
                    break
    return files
    
def main():
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)

    results = service.drives().list(pageSize=10).execute()
    shared_drive_id = results['drives'][0]['id']
    files = list_files_in_folder(service, shared_drive_id)

    signal.signal(signal.SIGINT, signal.SIG_IGN)

    cpu = os.cpu_count()
    pool = Pool(cpu)
    try:
        print("Mempersiapkan " + str(cpu) + " pekerja")
        pool.map_async(partial(startre, service), files)
        pool.close()
    except KeyboardInterrupt:
        print("\n\nMematikan proses")
        pool.terminate()
        print("Proses telah dimatikan!")
    except Exception:
        pool.terminate()
        print('pool is terminated')
    finally:
        pool.join()

def startre(service, files):
    file = service.files().get(fileId=files, supportsAllDrives=True, supportsTeamDrives=True).execute()
    try:
        print('Mengecek file ' + file['name'] + '('+ files +')')
        if (file['name'].startswith('Copy of ')):
            print('Mencoba merename file ' + file['name'] + '('+ files +')')
            judul_baru = file['name'].replace('Copy of ', '[Kiosnime] ')
            rename_file(service, files, judul_baru, file['name'])
    except KeyboardInterrupt:
        sys.exit()
    except:
        print('Terjadi kesalahan saat akan merename file ' + file['name'] + '('+ files +')')

if __name__ == '__main__':
    main()
    