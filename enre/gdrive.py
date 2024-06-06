import shutil
import tempfile
from functools import partial
from tqdm import tqdm
from multiprocessing import Pool
import os

from apiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account
from googleapiclient.discovery import build

from enre.settings import CREDENTIALS_FNAME, CREDENTIALS_ENV_VAR


class GDrive:
    def __init__(self):
        if CREDENTIALS_ENV_VAR in os.environ:
            # Load from env var if present
            # otherwise assume credentials are already in the file system
            credentials = os.environ[CREDENTIALS_ENV_VAR]
            with open(CREDENTIALS_FNAME, 'w') as f:
                f.write(credentials)

        if not os.path.exists(CREDENTIALS_FNAME):
            raise RuntimeError(f'missing credentials fname in {os.path.abspath(CREDENTIALS_FNAME)}')

        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FNAME,
            scopes=['https://www.googleapis.com/auth/drive']
        )

        self.drive_service = build('drive', 'v3', credentials=credentials)

    def upload_fname(self, fname, verbose=True):
        file_metadata = {'name': fname}
        media = MediaFileUpload(fname, mimetype='text/plain')
        file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        if verbose: print(f"File ID: {file.get('id')}")

    def download_file(self, file, path, skip_existing=False):
        file_id = file['id']
        if skip_existing and os.path.exists(os.path.join(path, file['name'])):
            return False

        request = self.drive_service.files().get_media(fileId=file_id)
        output_fname = os.path.join(path, os.path.basename(file['name']))

        tmp_fname = tempfile.mktemp()
        try:
            with open(tmp_fname, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    return True
            shutil.move(tmp_fname, output_fname)
        finally:
            if os.path.exists(tmp_fname): os.unlink(tmp_fname)


    def download_all_files_parallel(self, path, skip_existing, processes=10, print_progress=True):
        response = self.drive_service.files().list().execute()

        if print_progress:
            total_progress = tqdm(desc='Downloading', unit=' pages')
        files = response['files']
        page_no = 1
        total_downloaded = 0
        while response.get('nextPageToken'):
            downloaded = 0
            with Pool(processes) as p:
                compute_iterator = p.imap_unordered(
                    partial(self.download_file, path=path, skip_existing=skip_existing), files
                )
                # if print_progress: compute_iterator = tqdm(compute_iterator, total=len(files))
                downloaded += sum(compute_iterator)
            total_downloaded += downloaded

            # print(f'Downloaded {downloaded} files from page {page_no}')

            response = self.drive_service.files().list(pageToken=response['nextPageToken']).execute()
            files = response['files']
            page_no += 1
            if print_progress:
                total_progress.set_description(f'Total downloaded: {total_downloaded}, covered', refresh=False)
                total_progress.update()

        # print('Finished Downloading')

    def download_all_files_seq(self, path, skip_existing):
        """
        old implementation of download files, unless for debugging, use `download_all_files`
        """
        response = self.drive_service.files().list().execute()

        files = response['files']
        page_no = 1
        while response.get('nextPageToken'):
            downloaded = 0
            for file in tqdm(files):
                downloaded += self.download_file(file, path, skip_existing=skip_existing)
            print(f'Downloaded {downloaded} files from page {page_no}')

            response = self.drive_service.files().list(pageToken=response['nextPageToken']).execute()
            files = response['files']
            page_no += 1

        print('Finished Downloading')
