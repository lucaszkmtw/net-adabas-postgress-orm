# test_with_unittest.py

from unittest import TestCase
from models.models import Foja


class TryCreateFoja(TestCase):
    def test_crear_foja(self):
        foja_dict = {
            'id_cargo': 1,
            'nu_acto_alta': 1,
            'tp_acto_alta': 1,
            'dt_acto_alta': 1,
            'cd_motivo_alta': 1,
            'nu_acto_baja': 1,
            'tp_acto_baja': 1,
            'dt_acto_baja': 1,
            'cd_motivo_baja': 1,
            'cd_empleador': 1,
            'nu_cuil': 1,
            'dt_ingreso': 1,
            'cd_Usuario_Alta': 1,
            'dt_Alta': 1
        }
        foja = Foja(foja_dict)
        self.assertTrue(foja.__dict__['id_cargo'] == foja_dict['id_cargo'])
