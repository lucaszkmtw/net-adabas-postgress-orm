from datetime import datetime

from settings.config import USER as CD_USUARIO

from alive_progress import alive_bar

from models.models import Concepto
from models.models import Foja
from models.models import Salario
from models.models import CargaMesEmpleador
from models.models import File
from models.models import Liquidacion
from models.models import Numeros
from models.utils import group_by

from logins.login import logger


DT_ACTUAL = datetime.now()

file = File.process_salario_file('SALARIO1.DAT')
salarios = Salario.objects.filter(file_id=file.file_hash, is_loaded=False)

if not salarios:
    raise Exception('Salario\'s file already loaded.')
"""
Chequea antes si ya se cargaron estos salarios
"""
try:
    foja = Foja.objects.get(
        cd_empleador=salarios[0].cd_empleador,
        nu_cuil=salarios[0].nu_cuil,
        nu_cargo=salarios[0].nu_cargo)
except Exception as err:
    logger.error(err)
else:
    liquidacion = Liquidacion.objects.filter(
        table_year=salarios[0].periodo_liq.year,
        dt_desde_liq=salarios[0].periodo_liq,
        id_cargo=foja.id_cargo,
        nu_liquidacion=salarios[0].nu_liquidacion,
        tp_ddjj=salarios[0].tp_ddjj)

    if liquidacion:
        raise Exception("""Ya existe la liquidacion
            periodo:{},
            id_cargo:{},
            nu_liquidacion:{},
            tp_ddjj:{}""".format(
            salarios[0].periodo_liq,
            foja.id_cargo,
            salarios[0].nu_liquidacion,
            salarios[0].tp_ddjj)
        )

total = len(salarios)
logger.info(f"{total} salarios.")

headers = ['cd_empleador', 'nu_cuil', 'nu_cargo']
salarios_group = group_by(salarios, headers)


# Reservar ids para cargar conceptos
concepto_id = Numeros.obtener_ultimo_nro('concepto')
Numeros.set_num('concepto', concepto_id + total)


with alive_bar(total) as progress_bar:
    progress_bar()
    if cme := CargaMesEmpleador.objects.get(id_carga=110061):
        try:
            for filtro_foja, salarios in salarios_group.items():
                try:
                    cd_empleador, nu_cuil, nu_cargo = filtro_foja.split(',')
                except ValueError as err:
                    logger.error(err)
                    raise err
                except Exception as err:
                    raise Exception(err)
                try:
                    foja = Foja.objects.get(
                        cd_empleador=cd_empleador,
                        nu_cuil=nu_cuil,
                        nu_cargo=nu_cargo)
                except Exception as err:
                    logger.error(err)
                    print(err)
                else:
                    liquidacion = Liquidacion(
                        table_year=salarios[0].periodo_liq.year,
                        id_cargo=foja.id_cargo,
                        nu_liquidacion=salarios[0].nu_liquidacion,
                        tp_ddjj=salarios[0].tp_ddjj,
                        dt_desde_liq=salarios[0].periodo_liq,
                        dt_hasta_liq=salarios[0].periodo_liq,
                        tp_liquidacion=salarios[0].tp_liquidacion,
                        cd_usuario_alta=CD_USUARIO)
                    liquidacion.save()

                    for salario in salarios:
                        concepto_id += 1
                        concepto = Concepto(
                            table_year=salario.periodo_liq.year,
                            cd_concepto=salario.cd_concepto_ips,
                            id_cargo=foja.id_cargo,
                            tp_ddjj=salario.tp_ddjj,
                            nu_liquidacion=salario.nu_liquidacion,
                            dt_desde_liq=salario.periodo_liq,
                            cd_concepto_emplea=salario.cd_concepto_emp,
                            tp_concepto=salario.tp_concepto,
                            ds_concepto=salario.ds_concepto,
                            cd_usuario_alta=CD_USUARIO,
                            dt_alta=DT_ACTUAL,
                            nu_importe=salario.qt_importe * (-1)
                            if salario.qt_importe_a_signo == '-'
                            else salario.qt_importe,
                            id_concepto=concepto_id)
                        concepto.save()
                        salario.is_loaded = True
                        salario.save(update=True)

                        progress_bar()

        except IndexError:
            print('FIN')

Numeros.set_num('concepto', concepto_id)
