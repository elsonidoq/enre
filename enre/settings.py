import os


here = os.path.dirname(__file__)

DATA_PATH = os.path.abspath(os.path.join(here, '../notebooks/data'))
CREDENTIALS_FNAME = os.path.abspath(os.path.join(here, '../credentials.json'))
CREDENTIALS_ENV_VAR = 'GDRIVE_SECRET'

# ensure exists
if not os.path.exists(DATA_PATH):
    os.makedirs(DATA_PATH)
