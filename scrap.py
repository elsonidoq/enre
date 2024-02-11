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
url_clima = 'https://api.cammesa.com/demanda-svc/demanda/ObtieneDemandaYTemperaturaRegion?id_region={region}'

def download_cortes():
    return requests.get(url_cortes).content.decode('utf8')


def download_clima():
    regiones = [1077, 1078]
    res = {}
    for r in regiones:
        res[r] = requests.get(url_clima.format(region=r)).content.decode('utf8')
    return res

def upload_fname(drive_service, fname):
    file_metadata = {'name': fname}
    media = MediaFileUpload(fname, mimetype='text/plain')
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    print(f"File ID: {file.get('id')}")


def main():
    now = datetime.now()
    fname = f"cortes-enre-{now.strftime('%Y-%m-%dT%H')}.txt"
    with open(fname, 'w') as f:
        f.write(download_cortes())

    fnames = [fname]
    
    for region, region_txt in download_clima().items():
        fname = f"demanda-enre-{now.strftime('%Y-%m-%dT%H')}-{region}.txt"
        with open(fname, 'w') as f:
            f.write(region_txt)
            
        fnames.append(fname)
    
    drive_service = setup_gdrive('GDRIVE_SECRET')
    
    for fname in fnames:
        upload_fname(drive_service, fname)
        

if __name__ == '__main__': 
    main()
