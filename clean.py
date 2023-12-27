from datetime import datetime
from turtle import update

from settings.config import USER as CD_USUARIO

from models.models import Concepto
from models.models import Foja
from models.models import Salario
from models.models import File
from models.models import Liquidacion
from models.utils import group_by

from logins.login import logger


DT_ACTUAL = datetime.now()

file = File.process_salario_file('SALARIO1.DAT')
salarios = Salario.objects.filter(file_id=file.file_hash, is_loaded=True)


headers = ['cd_empleador', 'nu_cuil', 'nu_cargo']
salarios_group = group_by(salarios, headers)


for filtro_foja, salarios in salarios_group.items():
    try:
        cd_empleador, nu_cuil, nu_cargo = filtro_foja.split(',')
        foja = Foja.objects.get(cd_empleador=cd_empleador,
                                nu_cuil=nu_cuil,
                                nu_cargo=nu_cargo)
    except Exception as err:
        logger.error(err)
    else:
        liquidacion = Liquidacion.objects.delete(
            id_cargo=foja.id_cargo,
            nu_liquidacion=salarios[0].nu_liquidacion,
            tp_ddjj=salarios[0].tp_ddjj,
            dt_desde_liq=salarios[0].periodo_liq,
        )

        for salario in salarios:
            conceptos = Concepto.objects.filter(
                table_year=salario.periodo_liq.year,
                cd_concepto=salario.cd_concepto_ips,
                id_cargo=foja.id_cargo,
                tp_ddjj=salario.tp_ddjj,
                nu_liquidacion=salario.nu_liquidacion,
                dt_desde_liq=salario.periodo_liq,
                cd_concepto_emplea=salario.cd_concepto_emp,
                tp_concepto=salario.tp_concepto,
                ds_concepto=salario.ds_concepto,
                cd_usuario_alta=CD_USUARIO)
            ids_conceptos = [c.id_concepto for c in conceptos]
            if ids_conceptos:
                Concepto.objects.delete(id_concepto__in=ids_conceptos)
            salario.is_loaded = False
            salario.save(update=True)
