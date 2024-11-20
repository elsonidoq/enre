import requests
from datetime import datetime

import os
from apiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account
from googleapiclient.discovery import build




def setup_gdrive():
    # with open('credentials.json', 'w') as f:
    #     f.write(os.environ[credentials_secret])
        
    # Load the credentials from the JSON key file
    credentials = service_account.Credentials.from_service_account_file(
        '../enre-414003-b35e6a909577.json',
        scopes=['https://www.googleapis.com/auth/drive']
    )

    drive_service = build('drive', 'v3', credentials=credentials)
    return drive_service


def download_file(file, path, skip_existing=False):
    file_id = file['id']
    if skip_existing and os.path.exists(os.path.join(path, file['name'])):
        return False
       
    drive_service = setup_gdrive()

    request = drive_service.files().get_media(fileId=file_id)    
    with open(os.path.join(path, os.path.basename(file['name'])), 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            return True

from functools import partial
from tqdm import tqdm
from multiprocessing import Pool


def download_all_files(path, skip_existing):
       
    drive_service = setup_gdrive()
    

    response = drive_service.files().list().execute()
    
    files = response['files']
    page_no = 1
    while response.get('nextPageToken'):
        downloaded = 0
        with Pool(10) as p:
            downloaded += sum(
                tqdm(p.imap_unordered(partial(download_file, path=path, skip_existing=skip_existing), files))
            )
        
        print(f'Downloaded {downloaded} files from page {page_no}')
        
        response = drive_service.files().list(pageToken=response['nextPageToken']).execute()
        files = response['files']
        page_no += 1
        
    print('Finished Downloading')
    
    
def download_all_files_seq(path, skip_existing):
       
    drive_service = setup_gdrive()
    

    response = drive_service.files().list().execute()
    
    files = response['files']
    page_no = 1
    while response.get('nextPageToken'):
        downloaded = 0
        for file in tqdm(files):
            downloaded += download_file(file, path, skip_existing=skip_existing)
        print(f'Downloaded {downloaded} files from page {page_no}')
        
        response = drive_service.files().list(pageToken=response['nextPageToken']).execute()
        files = response['files']
        page_no += 1
        
    print('Finished Downloading')


def main():
    download_all_files('data', skip_existing=True)

if __name__ == '__main__':
    main()