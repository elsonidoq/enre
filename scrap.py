import argparse
import gzip
import pytz
import requests
from datetime import datetime
import os
from apiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account
from googleapiclient.discovery import build


def setup_gdrive(credentials_secret):
    with open('credentials.json', 'w') as f:
        f.write(os.environ[credentials_secret])
        
    # Load the credentials from the JSON key file
    credentials = service_account.Credentials.from_service_account_file(
        'credentials.json',
        scopes=['https://www.googleapis.com/auth/drive']
    )

    drive_service = build('drive', 'v3', credentials=credentials)
    return drive_service

url_cortes = 'https://www.enre.gov.ar/mapaCortes/datos/Datos_PaginaWeb.js?11'
url_demanda = 'https://api.cammesa.com/demanda-svc/demanda/ObtieneDemandaYTemperaturaRegion?id_region={region}'
url_clima = 'https://www.enre.gov.ar/Graficos/UFS/data/Datos_UFS.js?16'

def download_cortes():
    return requests.get(url_cortes).content.decode('utf8')


def download_clima():
    return requests.get(url_clima).content.decode('utf8')


def download_demanda():
    regiones = [1077, 1078]
    res = {}
    for r in regiones:
        res[r] = requests.get(url_demanda.format(region=r)).content.decode('utf8')
    return res

def upload_fname(drive_service, fname):
    file_metadata = {'name': fname}
    media = MediaFileUpload(fname, mimetype='text/plain')
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"File ID: {file.get('id')}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', default=False, action='store_true')
    parser.add_argument('--cortes', default=False, action='store_true')
    parser.add_argument('--clima', default=False, action='store_true')
    parser.add_argument('--demanda', default=False, action='store_true')

    args = parser.parse_args()

    now = datetime.now(tz=pytz.timezone('America/Argentina/Buenos_Aires'))

    fnames = []

    if args.cortes:
        # CORTES
        fname = f"cortes-enre-{now.strftime('%Y-%m-%dT%H:%M')}.txt.gz"
        with gzip.open(fname, 'w') as f:
            f.write(download_cortes().encode('utf8'))
        fnames.append(fname)

    if args.clima:
        # CLIMA
        fname = f"clima-enre-{now.strftime('%Y-%m-%dT%H:%M')}.txt.gz"
        with gzip.open(fname, 'w') as f:
            f.write(download_clima().encode('utf8'))
        fnames.append(fname)

    if args.demanda:
        # DEMANDA
        for region, region_txt in download_demanda().items():
            fname = f"demanda-enre-{now.strftime('%Y-%m-%dT%H:%M')}-{region}.txt.gz"
            with gzip.open(fname, 'w') as f:
                f.write(region_txt.encode('utf8'))
                
            fnames.append(fname)
    
    if not args.dry_run:
        drive_service = setup_gdrive('GDRIVE_SECRET')
        
        for fname in fnames:
            upload_fname(drive_service, fname)
        


if __name__ == '__main__': 
    main()
