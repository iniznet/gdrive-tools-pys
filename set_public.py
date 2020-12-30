from __future__ import print_function
import httplib2
import os
import json
import pickle
import sys
import time
import psutil
import signal

from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools
from oauth2client import file
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import multiprocessing
from functools import partial

import pandas as pd
from openpyxl import load_workbook
from googleapiclient import errors

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

def exit_handler(exctype, value, traceback):
    if exctype == KeyboardInterrupt:
        print("Mematikan...")
    else:
        sys.__excepthook__(exctype, value, traceback)
sys.excepthook = exit_handler

SCOPES = ['https://www.googleapis.com/auth/drive']
credentials_json = 'oauth-credentials'
credentials_pickle = 'token'
# Ganti ke jumlah akun yang kamu punya
akun_maksimal = 7

parent_id = os.getpid()
def worker_init():
    def sig_int(signal_num, frame):
        print('signal: %s' % signal_num)
        parent = psutil.Process(parent_id)
        for child in parent.children():
            if child.pid != os.getpid():
                print("killing child: %s" % child.pid)
                child.kill()
        print("killing parent: %s" % parent_id)
        parent.kill()
        print("suicide: %s" % os.getpid())
        psutil.Process(os.getpid()).kill()
    signal.signal(signal.SIGINT, sig_int)

def get_credentials(number):
    creds = None
    creds_file = credentials_json + str(number) + '.json'
    creds_pickle = credentials_pickle + str(number) + '.pickle'

    print("["+ format(time.strftime("%H:%M:%S")) +"] Membuka " + creds_file + " dan " + creds_pickle) 

    # Obtain OAuth token / user authorization.
    if os.path.exists(creds_pickle):
        with open(creds_pickle, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                creds_file, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(creds_pickle, 'wb') as token:
            pickle.dump(creds, token)
    return creds

def get_pickles(maxs):
    creds = None
    counter = 1
    print("["+ format(time.strftime("%H:%M:%S")) +"] Mempersiapkan bahan...")
    while counter <= maxs:
        creds_file = credentials_json + str(counter) + '.json'
        try_pickle = credentials_pickle + str(counter) + '.pickle'
        print("["+ format(time.strftime("%H:%M:%S")) +"] Cek Identitas " + str(counter))
        # If there are no (valid) credentials available, let the user log in.
        if not os.path.exists(try_pickle):
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    creds_file, SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(try_pickle, 'wb') as token:
                pickle.dump(creds, token)

        counter += 1
    return creds

def list_files_in_folder(service, folder_id, nama_folder):
    files = []
    jumlah = 0
    page_token = None

    query = "'" + folder_id + "' in parents and trashed = false"
    response1 = service.files().list(corpora="drive", driveId=folder_id, supportsAllDrives=True, includeItemsFromAllDrives=True, q=query, fields='files(id, name)', pageSize=10).execute()
    query = "mimeType = 'application/vnd.google-apps.folder' and name = '"+nama_folder+"' and trashed = false"
    response2 = service.files().list(corpora="drive", driveId=folder_id, supportsAllDrives=True, includeItemsFromAllDrives=True, q=query, fields='files(id, name)', pageSize=10).execute()
    query = "'" + response2['files'][0]['id'] + "' in parents and trashed = false"
    response3 = service.files().list(corpora="drive", driveId=folder_id, supportsAllDrives=True, includeItemsFromAllDrives=True, q=query, fields='files(id, name, fileExtension)', pageSize=1000).execute()
    print("\n["+ format(time.strftime("%H:%M:%S")) +"] Memuat Folder " + nama_folder)

    print("\n["+ format(time.strftime("%H:%M:%S")) +"] Mencoba mendapatkan semua file dari berbagai folder")
    for folder in response3['files']:
        try:
            thisType = folder['fileExtension']
        except:
            thisType = '0'
        if (folder['name'].startswith('.Anime') or not '0' in thisType):
            continue
        while True:
            try:
                param = {}
                if page_token:
                    param['nextPageToken'] = page_token
                children = service.files().list(supportsAllDrives=True, includeItemsFromAllDrives=True, q="'"+ folder['id'] +"' in parents and trashed = false", fields='files(id, name)', pageSize=1000, **param).execute()
                
                for child in children.get('files', []):
                    jumlah += 1
                    files.append(child['id'])
                print("["+ format(time.strftime("%H:%M:%S")) +"] Mendapatkan " + str(jumlah) + " file di dalam folder " + folder['name'] + " (" + folder['id'] + ")")
                page_token = children.get('nextPageToken')
                if not page_token:
                    jumlah = 0
                    break
            except KeyboardInterrupt:
                sys.exit(0)
            except:
                print('Terjadi kesalahan saat mendapatkan daftar semua file di folder ' + folder['name'] + '(' + folder['id'] + ')')
                break
    return files

def print_during(time_start):
    time_stop = time.time()
    hours, rem = divmod((time_stop - time_start), 3600)
    minutes, sec = divmod(rem, 60)
    print("Elapsed Time: {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), sec))

hasil = {}

def main():
    global hasil
    cred_count = 1

    folder = input("Nama Folder: ")

    time_start = time.time()
    get_creds = get_pickles(akun_maksimal)

    while cred_count <= akun_maksimal:
        print("\n\n["+ format(time.strftime("%H:%M:%S")) +"] Memakai identitas ke " + str(cred_count))
        creds = get_credentials(cred_count)

        service = build('drive', 'v3', credentials=creds)

        results = service.drives().list(pageSize=10).execute()
        shared_drive_id = results['drives'][0]['id']
        files = list_files_in_folder(service, shared_drive_id, folder)

        print("["+ format(time.strftime("%H:%M:%S")) +"] Ada " + str(len(files)) + " file di identitas " + str(cred_count))

        cpu = os.cpu_count()
        pool = multiprocessing.Pool(cpu, worker_init())
        try:
            print("\n["+ format(time.strftime("%H:%M:%S")) +"] Mempersiapkan " + str(cpu) + " pekerja\n")
            hasil = pool.map(partial(startre, service, cred_count), files)
        except KeyboardInterrupt:
            print('\n\nMematikan proses')
            pool.terminate()
            print("Proses telah dimatikan!")
            sys.exit(0)
        except:
            pool.terminate()
        finally:
            pool.close()
            pool.join()

        print("\n\n["+ format(time.strftime("%H:%M:%S")) +"] Semua file di Identitas " + str(cred_count) + " sudah terpublik!\n\n")

        saveto = 'GDrive ' + str(cred_count) + '.xlsx'
        save_sheet(cred_count, saveto, hasil)

        # Reset
        creds = None
        service = None
        results = None
        shared_drive_id = None
        files = None
        
        cred_count += 1
    
    print("\n\n\n["+ format(time.strftime("%H:%M:%S")) +"] All Done!!")
    print_during(time_start)

def startre(service, counter, files):
    file = service.files().get(fileId=files, supportsAllDrives=True, supportsTeamDrives=True, fields='id, name, webContentLink').execute()
    try:
        print('['+ format(time.strftime("%H:%M:%S")) +'] Mencoba mempublikan & mengambil link file ' + file['name'] + ' ('+ files +')')
        gopublic = public(service, files, file['name'])
        if gopublic:
            print('['+ format(time.strftime("%H:%M:%S")) +'] Berhasil! Mengoleksi nama dan link file '+ file['name'] +' ke identitas ' + str(counter))
            try:
                all_files = {
                    'Nama': file['name'],
                    'Link': file['webContentLink']
                }
                return all_files
            except:
                print('Gagal mengoleksi file ' + file['name'] +' ke identitas ' + str(counter))
    except:
        print('Terjadi kesalahan saat akan mempublikan & mengambil link file ' + file['name'] + ' ('+ files +')')

def save_sheet(identitas, xlsx, results):
    print('['+ format(time.strftime("%H:%M:%S")) +'] Menyimpan semua nama dan link file dari identitas '+ str(identitas) +' ke GDrive ' + str(identitas) + '.xlsx')
    semua = {
        'Nama': [],
        'Link': []
    }

    for elemen in results:
        for key, value in elemen.items():
            value.lstrip(' ')
            if (value.startswith('[Kiosnime] ')):
                value = value.replace('[Kiosnime] ', '')
                semua['Nama'].append(value)
            elif not(value.startswith('http')):
                semua['Nama'].append(value)
            else:
                semua['Link'].append(value)

    df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in semua.items() ]))
    df.sort_values(by=['Nama'], ascending=True, inplace=True)
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(xlsx, engine='xlsxwriter')

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(xlsx, engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object.
    df.to_excel(writer, sheet_name='Hasil', index=False)

    for column in df:
        column_length = max(df[column].astype(str).map(len).max(), len(column))
        col_idx = df.columns.get_loc(column)
        writer.sheets['Hasil'].set_column(col_idx, col_idx, column_length)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

def public(service, file_id, judul):
    try:
        print ("["+ format(time.strftime("%H:%M:%S")) +"] Mempublikan file " + judul)
        permission = {
            'role': 'reader',
            'type': 'anyone'
        }
        update_permission = service.permissions().create(fileId=file_id, body=permission, supportsAllDrives=True, supportsTeamDrives=True).execute()
        return update_permission
    except errors.HttpError as error:
        print('Internet lu kenapa tod kok Error saat ingin mempublikan file:\n', error)
    except:
        print('Error saat ingin mempublikan file ' + judul + ' (' + file_id + ')')
    return None

if __name__ == '__main__':
    main()
    