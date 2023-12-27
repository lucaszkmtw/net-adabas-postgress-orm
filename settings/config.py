import os
import getpass
from pathlib import Path

DEBUG = os.getenv('DEBUG', True)

DIRNAME = os.path.dirname(Path(__file__).parent)

DATA_ROOT = os.path.join(DIRNAME, 'data')

ADABAS_DATABASE = {
    'jclassname': 'de.sag.jdbc.adabasd.ADriver',
    'url': 'jdbc:adabasd://10.25.67.212/RUPABK/',
    'driver_args': {
        'user': 'UOL',
        'password': 'UOL',
    },
    'jars': os.path.join(DIRNAME, 'connections/adabasd.jar')
}

POSTGRES_DATABASE = {
    'database': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres'
}

POSTGRES_HOSTED_DATABASE = {
    'database': 'rupabk',
    'password': 'kdmf83y2d',
    'host': '10.25.67.205',
    'port': 5432,
    'user': 'rupa'
}

USER = getpass.getuser()
