from models.utils import filter

from models.models import Foja
from models.models import Salario

from logins.login import logger


fojas = list()

salarios = Salario.objects.all()
filtros = filter(salarios, ['cd_empleador', 'nu_cuil', 'nu_cargo'])

for cd_empleador, nu_cuil, nu_cargo in filtros:
    try:
        foja = Foja.objects\
            .using('adabas')\
            .get(cd_empleador=cd_empleador,
                 nu_cuil=nu_cuil,
                 nu_cargo=nu_cargo)

    except Exception as err:
        logger.info("No existe foja: cd_empleador={}, nu_cuil={}, nu_cargo={}"
                    .format(
                        cd_empleador,
                        nu_cuil,
                        nu_cargo)
                    )

    else:
        fojas.append(dict(foja))

Foja.objects.using('postgres').bulk_insert(fojas)

"""
python cargasap.py  4.04s user 0.67s system 8% cpu 58.114 total
probar con update true
"""
