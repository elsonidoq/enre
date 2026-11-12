import traceback
from collections import Counter

from tqdm import tqdm as terminal_tqm
from tqdm.notebook import tqdm as notebook_tqdm
import json
import re
import os
from datetime import timedelta


from datetime import timedelta, datetime
import gzip

from enre import settings


def parse_demanda(fname):
    region = int(fname.split('-')[-1].split('.')[0])
    open_func = gzip.open if fname.endswith('gz') else open

    with open_func(fname) as f:
        res = json.load(f)
        for doc in res:
            doc['region'] = region
            doc['fecha'] = datetime.strptime(doc['fecha'][:19], '%Y-%m-%dT%H:%M:%S')
    return res


def parse_clima(fname):
    open_func = gzip.open if fname.endswith('gz') else open

    with open_func(fname) as f:
        clima_content = f.read()

    if isinstance(clima_content, bytes): clima_content = clima_content.decode('utf8')

    data = clima_content.split(';')[0]
    parsed_data = json.loads(data[6:].replace("'", '"'))
    keys = ['hour', 'ufs_edesur', 'ufs_edenor', 'codigo', 'estado_cielo', 'temperatura']
    parsed_data = [dict(zip(keys, row)) for row in parsed_data]

    fname_date = parse_fname_date(fname)

    hour, minute = map(int, parsed_data[-1]['hour'].split(':'))
    parsed_data[-1]['datetime'] = fname_date.replace(hour=hour, minute=minute)

    for i in range(len(parsed_data) - 2, -1, -1):
        current_row = parsed_data[i]
        current_time = datetime.strptime(current_row['hour'], '%H:%M')

        prev_row = parsed_data[i + 1]
        prev_time = datetime.strptime(prev_row['hour'], '%H:%M')

        delta = (prev_time - current_time)
        if delta.total_seconds() < 0:
            delta += timedelta(days=1)

        current_row['datetime'] = prev_row['datetime'] - delta

    return parsed_data


def parse_fname_date(fname):
    date_pat = re.compile('(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})')
    if date_pat.search(fname) is None:
        raise ValueError('old fname')

    return datetime.strptime(date_pat.search(fname).group(0), '%Y-%m-%dT%H:%M')


def parse_cortes(fname):
    """
    convierte el json crudo de cortes en una lista de diccionarios (tambien parsea la descripccion)
    """
    open_func = gzip.open if fname.endswith('gz') else open
    with open_func(fname) as f:
        contents = f.read()

    if len(contents) == 0: return []

    if isinstance(contents, bytes): contents = contents.decode('utf8')

    var_pat = re.compile('addressPoints_Cuadro_D.*?=.*?(?P<lista>.*?);?$')
    lista = var_pat.search(contents).groupdict()['lista'].strip()[1:-1].strip()

    corte_pat = re.compile('(\[.*?\])')
    cortes = []
    fname_date = parse_fname_date(fname)
    keys = ['latitud', 'longitud', 'misc', 'descr']
    for corte_match in corte_pat.finditer(lista):
        corte = json.loads(corte_match.group(0))
        corte = dict(zip(keys, corte))

        lines = corte['descr'].split(',')
        descr_dict = {'tipo': lines[0], 'empresa': lines[1].strip()}
        for line in lines[2:]:
            colon_pos = line.find(':')
            k = line[:colon_pos].strip()
            v = line[colon_pos + 1:].strip()
            descr_dict[k.strip().lower()] = v.strip()

        corte.update(descr_dict)
        corte['fname_date'] = fname_date
        if fname_date.minute > 30:
            corte['date_hour'] = fname_date.replace(minute=0) + timedelta(hours=1)
        else:
            corte['date_hour'] = fname_date.replace(minute=0)

        cortes.append(corte)

    return cortes


def load_all(verbose=True, notebook=True, skip_errors=True):
    cortes = []
    demanda = []
    clima = []

    fnames = os.listdir(settings.DATA_PATH)
    if verbose:
        if notebook:
            fnames = notebook_tqdm(fnames)
        else:
            fnames = terminal_tqm(fnames)

    errors = Counter()
    for fname in fnames:
        try:
            parse_fname_date(fname)
        except ValueError:
            continue

        if fname.startswith('cortes'):
            parsing_func = parse_cortes
            output_list = cortes
        elif fname.startswith('clima'):
            parsing_func = parse_clima
            output_list = clima
        else:
            parsing_func = parse_demanda
            output_list = demanda

        fname = os.path.join(settings.DATA_PATH, fname)
        try:
            output_list.extend(parsing_func(fname))
        except Exception as e:
            if skip_errors:
                errors[type(e)] += 1
            else:
                raise e

    if errors:
        print('There were some errors')
        print('\n'.join(f'{k}: {v}' for k, v in errors.items()))

    return cortes, demanda, clima
