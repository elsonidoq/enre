import shutil
import tempfile
from functools import partial
from tqdm import tqdm
from multiprocessing import Pool, Process, Queue
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
                    shutil.move(tmp_fname, output_fname)
                    return True
        finally:
            if os.path.exists(tmp_fname): os.unlink(tmp_fname)

    def download_all_files_parallel_2(self, path, skip_existing, processes=10, print_progress=True):
        """
        utra fast implementation with producer process for pagination and process pool for concurent page fetch
        :return:
        """
        if print_progress:
            page_progress = tqdm(desc='Downloading', unit=' pages')
            files_progress = tqdm(desc='Downloading', unit=' files')

        producer = PagesProducer()
        try:
            producer.start()
            while producer.is_alive():
                files = producer.output_queue.get()
                with Pool(processes) as p:
                    compute_iterator = p.imap_unordered(
                        partial(self.download_file, path=path, skip_existing=skip_existing), files
                    )
                    if print_progress: compute_iterator = apply_tqdm(compute_iterator, files_progress)
                    # this waits for the computation to finish
                    sum(compute_iterator)

                if print_progress:
                    page_progress.update()

        finally:
            producer.kill()

    def download_all_files_parallel(self, path, skip_existing, processes=10, print_progress=True):
        """
        fast implementation with pool process to fetch a page
        """
        response = self.drive_service.files().list().execute()

        if print_progress:
            page_progress = tqdm(desc='Downloading', unit=' pages')
            files_progress = tqdm(desc='Downloading', unit=' files')
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
                page_progress.update()
                files_progress.update(downloaded)

    def download_all_files_seq(self, path, skip_existing):
        """
        old implementation of download files, unless for debugging or benchmarking use the other two ones
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


class PagesProducer(Process):
    def __init__(self):
        super().__init__()
        self.output_queue = Queue()

    def run(self) -> None:
        drive = GDrive()
        response = drive.drive_service.files().list().execute()
        files = response['files']
        self.output_queue.put_nowait(files)
        while response.get('nextPageToken'):
            response = drive.drive_service.files().list(pageToken=response['nextPageToken']).execute()
            self.output_queue.put_nowait(response['files'])


def apply_tqdm(it, progress):
    for e in it:
        progress.update()
        yield e