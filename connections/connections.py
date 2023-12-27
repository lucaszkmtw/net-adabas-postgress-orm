import psycopg2
import jaydebeapi

from settings.config import ADABAS_DATABASE
from settings.config import POSTGRES_DATABASE
from settings.config import POSTGRES_HOSTED_DATABASE

connections = {
    'adabas': jaydebeapi,
    'postgres': psycopg2,
}

connection_strings = {
    'adabas': ADABAS_DATABASE,
    'postgres': POSTGRES_DATABASE,
}

adabas_connection = connections['adabas']\
    .connect(**connection_strings['adabas'])
postgres_connection = connections['postgres']\
    .connect(**connection_strings['postgres'])


def connect(conection_name: str):
    if conection_name == 'adabas':
        return adabas_connection

    if conection_name == 'postgres':
        return postgres_connection

    raise Exception('No se encontro la conexion')
