import argparse
import gzip
import pytz
import requests
from datetime import datetime

from enre.lib.gdrive import GDrive

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
        gdrive = GDrive()

        for fname in fnames:
            gdrive.upload_fname(fname)


if __name__ == '__main__':
    main()
