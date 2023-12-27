import json
import os

from datetime import datetime
from dataclasses import dataclass
from dataclasses import fields
from psycopg2 import DatabaseError
from psycopg2.errors import UniqueViolation
from jaydebeapi import DatabaseError

from connections.connections import connect
from connections.connections import connections
from settings.config import DIRNAME
from settings.config import DEBUG
from logins.login import logger


PKS = json.loads(open(os.path.join(DIRNAME, 'settings/pks.json')).read())

DT_ACTUAL = datetime.now()


@dataclass
class BaseManager:
    connection = None
    model_class = None

    def __init__(self, model_class):
        if not model_class.Meta:
            raise ValueError('Model class must have Meta class')

        self.model_class = model_class

    @property
    def table_name(self):
        return self.model_class.get_table_name()

    @table_name.setter
    def table_name(self, table_name):
        return self.model_class.set_table_name(table_name)

    @property
    def default_db(self):
        return self.model_class.get_default_db()

    @default_db.setter
    def default_db(self, database_name):
        return self.model_class.set_default_db(database_name)

    @property
    def fields(self):
        return [field.name for field in fields(self.model_class)]

    @property
    def primary_key(self):
        return self.get_primary_key()

    def _get_cursor(self):
        if not self.connection:
            connection = connect(self.default_db)
            connection.autocommit = True
            self.connection = connection
        return self.connection.cursor()

    def _execute_query(self, query):
        cursor = self._get_cursor()
        if self.primary_key and self.default_db == 'postgres':
            query += " RETURNING " + self.primary_key

        if DEBUG:
            logger.info(query)
            print("\033[0;33m " + query + "\033[0m\n")

        try:
            cursor.execute(query)
        except DatabaseError as e:
            error = e.args[0].args[0]
            logger.error(f"{error}S on table {self.table_name}")
        except Exception as e:
            logger.error(e)
            raise e
        if cursor.description is None:
            return None
        else:
            fields = [field[0] for field in cursor.description]
            rows = [[r[0]] for r in cursor.fetchall()]
            return [dict(zip(fields, row)) for row in rows]

    def _get_last_id(self):
        from models.models import Numeros
        if self.default_db == 'adabas':
            return Numeros.obtener_ultimo_nro(
                tabla=self.table_name.upper())
        else:
            raise NotImplementedError

    def _clean_fields(self, fields, field_names, default_db):
        if '*' in field_names:
            field_names = fields

        for fn in fields:
            if fn not in fields:
                raise ValueError(
                    f"{self.__class__.__name__} has no field {fn}")

        if not default_db == 'adabas':
            if self.primary_key not in field_names:
                field_names = list(field_names)
                field_names.insert(0, self.primary_key)
                field_names = tuple(field_names)

        return (fields, field_names)

    def select(self, *field_names, chunk_size=2000, condition=None):
        """_summary_

        Args:
            chunk_size (int, optional): Tamanio del bloque en el que se va a
        procesar.
            Defaults to 2000.
            condition (Condition(), optional): Condition object. Defaults to
        None.
            default_db (_type_, optional): Default db. Defaults to None.

        Returns:
            list(objects): Devuelve una lista de los objetos encontrados.
        """
        _, field_names = self._clean_fields(
            self.fields, field_names, self.default_db)

        if condition:
            if 'table_year' in condition.__dict__:
                field_names.pop(field_names.index('table_year'))
                table_year_val = condition.__dict__.pop('table_year')
                if table_year_val != DT_ACTUAL.year:
                    self.table_name += str(table_year_val)

        fields_format = ', '.join(field_names)

        query = f"SELECT {fields_format} FROM {str(self.table_name)}"
        vars = []

        if condition:
            query += f" WHERE {condition.sql_format}"
            vars += condition.query_vars

        # Execute query
        query = query.replace('%s', '{}')
        query = query.format(*vars)
        if DEBUG:
            logger.info(query)
            print("\033[1;32m " + query + "\033[0m\n")

        cursor = self._get_cursor()
        try:
            cursor.execute(query)
        except Exception as e:
            error = str(e)
            logger.error(error)
            raise Exception(
                f"\033[1;31m {error} IN TABLE {self.table_name}\033[0m")
        model_objects = list()
        is_fetching_completed = False
        while not is_fetching_completed:

            if self.default_db == 'adabas':
                rows = cursor.fetchall()
                chunk_size = len(rows) + 1
            else:
                rows = cursor.fetchmany(size=chunk_size)

            is_fetching_completed = len(rows) < chunk_size

            for row in rows:
                row_data = dict(zip(field_names, row))
                model_objects.append(self.model_class(**row_data))

        return model_objects

    def get_primary_key(self):
        if self.default_db == 'adabas':
            return PKS[self.table_name.lower()]
        else:
            return self.model_class.Meta.primary_key

    def bulk_insert(self, data, get_pk=False):
        from models.utils import Condition
        if 'table_year' in data[0]:
            if data[0]['table_year'] != DT_ACTUAL.year:
                if not self.table_name.endswith(str(data[0]['table_year'])):
                    self.table_name += str(data[0]['table_year'])
            for i, _ in enumerate(data):
                del data[i]['table_year']
        field_names = list(data[0].keys())
        values = list()
        for row in data:
            assert list(row.keys()) == field_names
            cleaned_fields = [Condition._clean_fields(
                row[fn]) for fn in field_names]
            values.append(f"({','.join(cleaned_fields)})")

        if get_pk:
            last_id_list = list()
            field_names.append(self.primary_key)

            for i in range(len(values)):
                last_id = self._get_last_id()
                last_id_list.append({self.primary_key: last_id})
                values[i] = f"{values[i][:-1]}, {last_id})"

        values_str = ", ".join(values)

        fields_str = ', '.join(field_names)
        query = f"INSERT INTO {self.table_name} ({fields_str}) \
                  VALUES {values_str}"
        valores = self._execute_query(query)
        if not valores and get_pk:
            return last_id_list
        else:
            return valores

    def insert(self, **row_data):
        return self.bulk_insert(data=[row_data])

    def update(self, new_data, condition=None):
        from models.utils import Condition

        new_data_format = ', '.join(
            [f'{field_name} = {Condition._clean_fields(value)}'
                for field_name, value in new_data.items()])
        query = f"UPDATE {self.table_name} SET {new_data_format}"
        vars = []
        if condition:
            query += f" WHERE {condition.sql_format}"
            vars += condition.query_vars

        query = query.replace('%s', '{}')
        query = query.format(*vars)

        return self._execute_query(query)

    def _delete(self, condition=None):
        query = f"DELETE FROM {self.table_name} "
        vars = []
        if condition:
            query += f" WHERE {condition.sql_format}"
            vars += condition.query_vars

        query = query.replace('%s', '{}')
        query = query.format(*vars)
        return self._execute_query(query)

    def all(self):
        return self.select('*')

    def filter(self, **kwargs):
        from models.utils import Condition
        return self.select('*',
                           condition=Condition(
                               default_db=self.default_db,
                               **kwargs)
                           )

    def create(self, **data):
        fields = self.bulk_insert([data])
        if fields:
            for field in fields:
                filtro = dict()
                if type(self.primary_key) == str:
                    filtro[self.primary_key] = field[self.primary_key]
                else:
                    for key in self.primary_key:
                        filtro[key] = field[key]
                return self.get(**filtro)

    def get_or_create(self, data):
        try:
            return self.create(**data)
        except UniqueViolation:
            return self.get(**{self.primary_key: data[self.primary_key]})
        except Exception as e:
            raise e

    def save(self, data: dict = dict(), update=False):
        from models.utils import Condition
        if update:
            condition = self._get_conditions(data)
            return self.update(data, condition)
        return self.get_or_create(data)

    def get(self, **kwargs):
        result = self.filter(**kwargs)
        if len(result) > 1:
            raise ValueError(f'More than one {self.table_name} found.')
        elif result:
            return result[0]
        else:
            raise Exception(f'{self.table_name.capitalize()} not found.')

    def _get_conditions(self, kwargs):
        from models.utils import Condition
        if 'table_year' in kwargs:
            table_year_val = kwargs.pop('table_year')
            if table_year_val != DT_ACTUAL.year:
                self.table_name += str(table_year_val)
        if not self.default_db == 'postgres':
            indexes = PKS[self.table_name.lower()]
        else:
            indexes = ['id']

        if not any(['__' in cond for cond in kwargs]):
            conditions = {i: kwargs[i] for i in indexes}
        else:
            field_list = [cond.split('__')[0] for cond in kwargs]
            if any([i in field_list for i in indexes]):
                # ULTRA FIXME
                conditions = kwargs
            else:
                raise Exception('no se encontro el indice in ...')

        return Condition(
            default_db=self.default_db,
            **conditions
        )

    def delete(self, **kwargs):
        return self._delete(self._get_conditions(kwargs))

    def raw(self, query):
        return self._execute_query(query)

    def using(self, database_name):
        if database_name in connections.keys():
            self.default_db = database_name
            return self
        else:
            raise Exception(f'{database_name} not a valid database name. ')


def update_pks():
    query = "SELECT TABLENAME, COLUMNNAME \
             FROM SHOW_PRIMARY_KEY \
             WHERE OWNER = 'IPSGRP'"
    cursor = connect('adabas').cursor()
    cursor.execute(query)

    with open(os.path.join(DIRNAME, 'settings/pks.json'), 'w') as f:
        primary_keys = dict()
        for table, column in cursor.fetchall():
            if table.lower() not in primary_keys:
                primary_keys[table.lower()] = [column.lower()]
            else:
                primary_keys[table.lower()].append(column.lower())

        f.write(
            json.dumps(
                primary_keys,
                indent=4,
                sort_keys=True
            )
        )


def update_constrains():
    query = "SELECT TABLENAME, COLUMNNAME, INDEXNAME FROM SHOW_INDEX"
    cursor = connect('adabas').cursor()
    cursor.execute(query)

    with open(os.path.join(DIRNAME, 'settings/indexes.json'), 'w') as f:
        indexes = dict()
        for table, column, index_name in cursor.fetchall():
            if table.lower() not in indexes:
                indexes[table.lower()] = [column.lower()]
            else:
                indexes[table.lower()].append(column.lower())

        f.write(
            json.dumps(
                indexes,
                indent=4,
                sort_keys=True
            )
        )
