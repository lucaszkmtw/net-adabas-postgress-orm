from dataclasses import dataclass, field, asdict
from datetime import datetime
from datetime import date
from typing import Any

from .manager import BaseManager


class MetaModel(type):
    manager_class = BaseManager

    @property
    def objects(cls):
        return cls.manager_class(model_class=cls)


@dataclass
class BaseModel(metaclass=MetaModel):
    class Meta:
        table_name = None
        default_db = None
        primary_key = None

    def __setattr__(self, __name: str, __value: Any) -> None:
        """_summary_
        Misc:
            obj_types[__name]: Es el tipo de dato que se espera.
            type(__value): Es el tipo de dato que se está recibiendo.
        Args:
            __name (str): Nombre del atributo
            __value (Any): Valor del atributo

        Raises:
            TypeError: En el caso de que el tipo de dato no coincida con el
        esperado.
        """
        obj_types = self.__class__.__annotations__
        if __value is not None or __name == '_meta':
            if str(type(__value)) == "<java class 'java.lang.Boolean'>":
                __value = bool(__value)

            if str(type(__value)) == "<java class 'JLong'>":
                __value = int(__value)

            if obj_types[__name] == date:
                if type(__value) == str:
                    __value = datetime.strptime(
                        __value.split(' ')[0], '%Y-%m-%d').date()

            if str(obj_types[__name]) == "<class 'datetime.datetime'>" \
                    and type(__value) == str:
                __value = datetime.strptime(
                    __value.split('.')[0], '%Y-%m-%d %H:%M:%S')

            if obj_types[__name] == float:
                if type(__value) == int:
                    __value = float(__value)

                if str(type(__value)) == "<java class 'JDouble'>":
                    __value = float(__value)

            if obj_types[__name] == int:
                if type(__value) == str:
                    __value = int(__value)

            if obj_types[__name] != type(__value):
                raise TypeError(
                    f"In {self.__class__.__name__} {__name} must be of type \
                        {obj_types[__name]} and its {type(__value)}")

        super().__setattr__(__name, __value)

    def __dict__(self):
        obj_dict = asdict(self)
        return obj_dict

    def __iter__(self):
        return iter(self.__dict__().items())

    @classmethod
    def get_table_name(cls):
        return cls.Meta.table_name

    @classmethod
    def set_table_name(cls, table_name: str):
        cls.Meta.table_name = table_name

    @classmethod
    def get_default_db(cls):
        return cls.Meta.default_db

    @classmethod
    def set_default_db(cls, default_db: str):
        cls.Meta.default_db = default_db

    @classmethod
    def get_primary_key(cls):
        return cls.Meta.primary_key

    def save(self, update=False):
        return self.__class__.objects.save(data=dict(self), update=update)

    def delete(self):
        self.__class__.objects.delete(**self.__dict__)

    def using(self, db_name):
        self.Meta.default_db = db_name
        return self

    @classmethod
    def CharField(cls, *args, **kwargs):
        return field(default='')

    @classmethod
    def BooleanField(cls, *args, **kwargs):
        return field(default=False)

    @classmethod
    def DateField(cls, auto_now=False, auto_now_add=False, *args, **kwargs):
        return field(
            default=datetime.now().date() if auto_now else None
        )

    @classmethod
    def DateTimeField(cls, auto_now=False, auto_now_add=False, *args, **kwargs):
        return field(
            default=datetime.now() if auto_now else None,
        )

    @classmethod
    def IntegerField(cls, *args, **kwargs):
        return field(default=0)

    @classmethod
    def FloatField(cls, *args, **kwargs):
        return field(default=0)

    @classmethod
    def obtener_ultimo_id(cls):
        from models.models import Numeros
        return Numeros.obtener_ultimo_nro(cls.Meta.table_name.upper())
