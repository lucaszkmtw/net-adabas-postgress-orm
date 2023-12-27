import hashlib
import datetime as dt

from datetime import datetime
from dataclasses import dataclass

from psycopg2.errors import UniqueViolation

from models.model import BaseModel
from settings.config import DATA_ROOT
from settings.config import USER


@dataclass
class Foja(BaseModel):
    cd_empleador: str = BaseModel.CharField(max_length=8, null=False)
    cd_motivo_alta: int = BaseModel.IntegerField(max_length=1)
    cd_motivo_baja: int = BaseModel.IntegerField(max_length=2)
    cd_subvencionado: bool = BaseModel.BooleanField()
    cd_usuario_alta: str = BaseModel.CharField(max_length=10)
    ds_clave_prov: str = BaseModel.CharField(max_length=10)
    ds_escalafon_vig: str = BaseModel.CharField(max_length=50)
    ds_observacion: str = BaseModel.CharField(max_length=250)
    dt_acto_alta: dt.date = BaseModel.DateField()
    dt_acto_baja: dt.date = BaseModel.DateField()
    dt_actual: datetime = BaseModel.DateTimeField()
    dt_alta: datetime = BaseModel.DateTimeField()
    dt_ingreso: dt.date = BaseModel.DateField(null=False)
    id_cargo: int = BaseModel.IntegerField(
        null=False, primary_key=True, max_length=10)
    id_certificacion: int = BaseModel.IntegerField(max_length=11)
    nu_acto_alta: int = BaseModel.IntegerField(max_length=5)
    nu_acto_baja: int = BaseModel.IntegerField(max_length=5)
    nu_cargo: int = BaseModel.IntegerField(max_length=4)
    nu_cue: int = BaseModel.IntegerField(max_length=9)
    nu_cuil: int = BaseModel.IntegerField(max_length=11, null=False)
    nu_legajo: str = BaseModel.CharField(max_length=20)
    tp_acto_alta: int = BaseModel.IntegerField(max_length=1)
    tp_acto_baja: int = BaseModel.IntegerField(max_length=1)

    class Meta:
        table_name = 'foja'
        default_db = 'adabas'
        primary_key = 'id_cargo'


@dataclass
class Salario(BaseModel):
    id: int = BaseModel.IntegerField()
    is_loaded: bool = BaseModel.BooleanField()
    nu_cuil: int = BaseModel.IntegerField()
    cd_empleador: str = BaseModel.CharField()
    nu_cargo: int = BaseModel.IntegerField()
    periodo_liq: dt.date = BaseModel.DateField()
    nu_liquidacion: int = BaseModel.IntegerField()
    tp_liquidacion: str = BaseModel.CharField()
    tp_ddjj: str = BaseModel.CharField()
    cd_concepto_ips: str = BaseModel.CharField()
    cd_concepto_emp: str = BaseModel.CharField()
    tp_concepto: str = BaseModel.CharField()
    ds_concepto: str = BaseModel.CharField()
    qt_importe_a_signo: str = BaseModel.CharField()
    qt_importe: int = BaseModel.IntegerField()
    nu_anios_antiguedad_ips: int = BaseModel.IntegerField()
    nu_meses_antiguedad_ips: int = BaseModel.IntegerField()
    nu_anios_antiguedad_otras: int = BaseModel.IntegerField()
    nu_meses_antiguedad_otras: int = BaseModel.IntegerField()
    nu_anios_antiguedad_doc: int = BaseModel.IntegerField()
    nu_meses_antiguedad_doc: int = BaseModel.IntegerField()
    cd_forma_pago: str = BaseModel.CharField()
    nu_inasistencia_con_desc: int = BaseModel.IntegerField()
    nu_lic_sin_sueldo: int = BaseModel.IntegerField()
    cd_encuadre_previs: str = BaseModel.CharField()
    cd_modalidad_revista: str = BaseModel.CharField()
    file_id: str = BaseModel.CharField()

    class Meta:
        table_name = 'salario'
        default_db = 'postgres'
        primary_key = 'id'

    def _dict_from_line(linea: str, file_id=None):
        periodo_liq = datetime.strptime(
            linea[22:29] + '/01 00:00:00.00000',
            "%Y/%m/%d %H:%M:%S.%f")
        signo = -1 if linea[75:76] == '-' else 1
        return {
            'cd_empleador': linea[0:7].replace(' ', ''),
            'nu_cuil': int(linea[7:18]),
            'nu_cargo': int(linea[18:22]),
            'periodo_liq': periodo_liq,
            'nu_liquidacion': int(linea[29:31]),
            'tp_liquidacion': linea[31:32],
            'tp_ddjj': linea[32:33],
            'cd_concepto_ips': linea[33:39],
            'cd_concepto_emp': linea[39:47],
            'tp_concepto': linea[47:50],
            'ds_concepto': linea[50:75],
            'qt_importe_a_signo': linea[75:76],
            'qt_importe': float(f'{linea[76:83]}.{linea[83:85]}') * signo,
            'nu_anios_antiguedad_ips': int(linea[85:87]),
            'nu_meses_antiguedad_ips': int(linea[87:89]),
            'nu_anios_antiguedad_otras': int(linea[89:91]),
            'nu_meses_antiguedad_otras': int(linea[91:93]),
            'nu_anios_antiguedad_doc': int(linea[93:95]),
            'nu_meses_antiguedad_doc': int(linea[95:97]),
            'cd_forma_pago': linea[97:98],
            'nu_inasistencia_con_desc': int(linea[98:100]),
            'nu_lic_sin_sueldo': int(linea[100:102]),
            'cd_encuadre_previs': linea[102:104],
            'cd_modalidad_revista': linea[104:105],
            'file_id': file_id
        }


@dataclass
class Numeros(BaseModel):
    cd_usuario_actual: str = BaseModel.CharField()
    cd_usuario_alta: str = BaseModel.CharField()
    dt_actual: datetime = BaseModel.DateTimeField()
    dt_alta: datetime = BaseModel.DateTimeField()
    numero: int = BaseModel.IntegerField()
    tabla: str = BaseModel.CharField()

    class Meta:
        table_name = 'numeros'
        default_db = 'adabas'

    @classmethod
    def obtener_ultimo_nro(cls, tabla: str):
        """obtener_ultimo_nro

        Args:
            tabla ('str'): Nombre de la tabla de la cual se quiere obtener el
            ultimo id.

        Returns:
            _type_: devuelve un objecto Numeros() con el ultimo id de la tabla
        + 1.
        """
        tabla = tabla.upper()
        num = cls.objects.get(tabla=tabla)
        num.numero += 1
        num.dt_actual = datetime.now()
        num.cd_usuario_actual = USER
        num.save(update=True)
        return num.numero

    @classmethod
    def set_num(cls, tabla: str, numero: int):
        tabla = tabla.upper()
        num = cls.objects.get(tabla=tabla)
        num.numero = numero
        num.dt_actual = datetime.now()
        num.cd_usuario_actual = USER
        num.save(update=True)
        return num

    class ReservarID(object):
        def __init__(self, table):
            self.table = table

        def __enter__(self):
            self.file = open(self.file_name, 'w')
            return self.file

        def __exit__(self):
            self.file.close()


@dataclass
class File(BaseModel):
    file_name: str = BaseModel.CharField()
    uploaded_at: datetime = BaseModel.DateTimeField()
    file_hash: str = BaseModel.CharField()
    ended: bool = BaseModel.BooleanField()

    class Meta:
        table_name = 'files'
        default_db = 'postgres'
        primary_key = 'file_hash'

    @classmethod
    def process_salario_file(cls, filename: str):
        """proces_salario_file

        Args:
            filename (str): Nombre del archivo de salario

        Raises:
            e: En caso de error no contemplado.

        Returns:
            _type_: Devuelve un objeto File() con los datos del archivo.
        """
        salario_bulk = []
        with open(DATA_ROOT + '/' + filename, 'rb') as file4hash:
            file_hash = hashlib.sha1()
            chunk = 0
            while chunk != b'':
                # read only 1024 bytes at a time
                chunk = file4hash.read(1024)
                file_hash.update(chunk)
        try:
            file_obj = File.objects.create(
                file_name=filename,
                file_hash=file_hash.hexdigest()
            )
        except UniqueViolation:
            return File.objects.get(
                file_hash=file_hash.hexdigest()
            )
        else:
            try:
                with open(DATA_ROOT + '/' + filename, 'rb') as file:
                    for line in file:
                        salario_bulk.append(
                            Salario._dict_from_line(
                                line.decode('latin-1'),
                                file_obj.file_hash
                            )
                        )
                Salario.objects.bulk_insert(salario_bulk)
            except Exception as e:
                if file_obj:
                    file_obj.delete()
                raise e

            if len(salario_bulk) == 0:
                file_obj.delete()
            else:
                file_obj.ended = True
                file_obj.save()

            return file_obj


@dataclass
class Liquidacion(BaseModel):
    cd_usuario_alta: str = BaseModel.CharField()
    dt_alta: datetime = BaseModel.DateTimeField(auto_now=True)
    dt_desde_liq: dt.date = BaseModel.DateField()
    dt_hasta_liq: dt.date = BaseModel.DateField()
    id_cargo: int = BaseModel.IntegerField()
    nu_liquidacion: int = BaseModel.IntegerField()
    tp_ddjj: str = BaseModel.CharField()
    tp_liquidacion: str = BaseModel.CharField()
    table_year: int = BaseModel.IntegerField()

    class Meta:
        table_name = 'liquidacion'
        default_db = 'adabas'


@dataclass
class Cargo(BaseModel):
    cd_cargo: str = BaseModel.CharField()
    cd_encuadre_previs: int = BaseModel.IntegerField()
    cd_forma_pago: str = BaseModel.CharField()
    cd_modo_ocupa: int = BaseModel.IntegerField()
    cd_planta: int = BaseModel.IntegerField()
    cd_usuario_actual: str = BaseModel.CharField()
    cd_usuario_alta: str = BaseModel.CharField()
    ds_cargo: str = BaseModel.CharField()
    dt_actual: datetime = BaseModel.DateTimeField()
    dt_alta: datetime = BaseModel.DateTimeField()
    dt_baja: dt.date = BaseModel.DateField()
    dt_posesion: dt.date = BaseModel.DateField()
    id_cargo: int = BaseModel.IntegerField()
    id_cargoestatuto: int = BaseModel.IntegerField()
    id_cargomc: int = BaseModel.IntegerField()
    id_reparticion: int = BaseModel.IntegerField()

    class Meta:
        table_name = 'cargo'
        default_db = 'adabas'


@dataclass
class Afiliado(BaseModel):
    nu_cuil: int = BaseModel.IntegerField()
    tp_doc: int = BaseModel.IntegerField()
    nu_doc: int = BaseModel.IntegerField()
    ds_apellido_nombre: str = BaseModel.CharField()
    ds_nombre: str = BaseModel.CharField()
    dt_nacimiento: str = BaseModel.CharField()
    cd_nacionalidad: int = BaseModel.IntegerField()
    cd_sexo: str = BaseModel.CharField()
    cd_est_civil: int = BaseModel.IntegerField()
    cd_estudios: int = BaseModel.IntegerField()
    ds_profesion: str = BaseModel.CharField()
    nu_hijos: int = BaseModel.IntegerField()
    nu_antiguedad_ips: int = BaseModel.IntegerField()
    nu_antiguedad_otra: int = BaseModel.IntegerField()
    nu_domicilio: int = BaseModel.IntegerField()
    dt_fallecimiento: int = BaseModel.IntegerField()
    dt_baja: str = BaseModel.CharField()
    cd_causal_baja: str = BaseModel.CharField()
    ds_lugar_estudia: str = BaseModel.CharField()
    nu_antig_ips_meses: int = BaseModel.IntegerField()
    nu_antig_ips_dias: int = BaseModel.IntegerField()
    cd_usuario_alta: str = BaseModel.CharField()
    dt_alta: str = BaseModel.CharField()
    cd_usuario_actual: str = BaseModel.CharField()
    dt_actual: str = BaseModel.CharField()
    cd_fin_carga: bool = BaseModel.BooleanField()

    class Meta:
        table_name = 'afiliado'
        default_db = 'adabas'


@dataclass
class CargaMesEmpleador(BaseModel):
    cd_afiliado: bool = BaseModel.BooleanField()
    cd_datosemp: bool = BaseModel.BooleanField()
    cd_empleador: str = BaseModel.CharField()
    cd_fin: bool = BaseModel.BooleanField()
    cd_fin_afiliado: bool = BaseModel.BooleanField()
    cd_fin_datosemp: bool = BaseModel.BooleanField()
    cd_fin_revista: bool = BaseModel.BooleanField()
    cd_fin_salario: bool = BaseModel.BooleanField()
    cd_firma_digital: bool = BaseModel.BooleanField()
    cd_revista: bool = BaseModel.BooleanField()
    cd_salario: bool = BaseModel.BooleanField()
    cd_usuario_actual: str = BaseModel.CharField()
    cd_usuario_alta: str = BaseModel.CharField()
    cd_verificador: str = BaseModel.CharField()
    cd_vigente: bool = BaseModel.BooleanField()
    ds_archivo_anexo: int = BaseModel.IntegerField()
    ds_archivo_ddjj: str = BaseModel.CharField()
    ds_archivo_ddjj_u: str = BaseModel.CharField()
    ds_mensaje: str = BaseModel.CharField()
    ds_mensaje_afiliad: str = BaseModel.CharField()
    ds_mensaje_datosem: str = BaseModel.CharField()
    ds_mensaje_revista: str = BaseModel.CharField()
    ds_mensaje_salario: str = BaseModel.CharField()
    dt_actual: datetime = BaseModel.DateTimeField()
    dt_alta: datetime = BaseModel.DateTimeField()
    dt_carga: datetime = BaseModel.DateTimeField()
    dt_fin_carga: datetime = BaseModel.DateTimeField()
    dt_mes_liq: dt.date = BaseModel.DateField()
    id_carga: int = BaseModel.IntegerField()
    nu_liquidacion: int = BaseModel.IntegerField()
    nu_modif_afiliado: int = BaseModel.IntegerField()
    nu_modif_revista: int = BaseModel.IntegerField()
    nu_nuevos_afiliados: int = BaseModel.IntegerField()
    nu_nuevos_revista: int = BaseModel.IntegerField()
    nu_nuevos_salario: int = BaseModel.IntegerField()
    nu_personas_salari: int = BaseModel.IntegerField()
    nu_total_afiliado: int = BaseModel.IntegerField()
    nu_total_revista: int = BaseModel.IntegerField()
    nu_total_salario: int = BaseModel.IntegerField()
    qt_ajuste_patronal: int = BaseModel.IntegerField()
    tm_afiliado: int = BaseModel.IntegerField()
    tm_revista: int = BaseModel.IntegerField()
    tm_salario: int = BaseModel.IntegerField()
    tp_ddjj: str = BaseModel.CharField()
    tp_liquidacion: str = BaseModel.CharField()

    class Meta:
        table_name = 'cargamesempleador'
        default_db = 'adabas'


@dataclass
class CargarMesDetalle(BaseModel):
    cd_encuadre_previs: str = BaseModel.CharField()
    cd_planta: str = BaseModel.CharField()
    cd_usuario_actual: str = BaseModel.CharField()
    cd_usuario_alta: str = BaseModel.CharField()
    dt_actual: int = BaseModel.FloatField()
    dt_alta: int = BaseModel.FloatField()
    id_carga: int = BaseModel.FloatField()
    id_cargo: int = BaseModel.FloatField()
    nu_anios_doc: int = BaseModel.FloatField()
    nu_anios_ips: int = BaseModel.FloatField()
    nu_anios_otras: int = BaseModel.FloatField()
    nu_cuil: int = BaseModel.FloatField()
    nu_inasistencia: int = BaseModel.FloatField()
    nu_lic_sin_sueldo: int = BaseModel.FloatField()
    nu_meses_doc: int = BaseModel.FloatField()
    nu_meses_ips: int = BaseModel.FloatField()
    nu_meses_otras: int = BaseModel.FloatField()
    qt_afa: int = BaseModel.FloatField()
    qt_das: int = BaseModel.FloatField()
    qt_des: int = BaseModel.FloatField()
    qt_dps: int = BaseModel.FloatField()
    qt_liq: int = BaseModel.FloatField()
    qt_rca: int = BaseModel.FloatField()
    qt_rsa: int = BaseModel.FloatField()

    class Meta:
        table_name = 'cargamesdetalle'
        default_db = 'adabas'


@dataclass
class Concepto(BaseModel):
    cd_concepto: str = BaseModel.CharField()
    cd_concepto_emplea: str = BaseModel.CharField()
    cd_usuario_alta: str = BaseModel.CharField()
    ds_concepto: str = BaseModel.CharField()
    dt_alta: datetime = BaseModel.DateTimeField()
    dt_desde_liq: dt.date = BaseModel.DateField()
    id_cargo: int = BaseModel.IntegerField()
    id_concepto: int = BaseModel.IntegerField()
    nu_importe: float = BaseModel.FloatField()
    nu_liquidacion: float = BaseModel.FloatField()
    tp_concepto: str = BaseModel.CharField()
    tp_ddjj: str = BaseModel.CharField()
    table_year: int = BaseModel.IntegerField()

    class Meta:
        table_name = 'concepto'
        default_db = 'adabas'

    def save(self):
        # self.id_concepto = self.obtener_ultimo_id()
        super().save()


@dataclass
class ax_mod_revista(BaseModel):
    cd_ocupa: int = BaseModel.IntegerField()
    cd_planta: int = BaseModel.IntegerField()
    cd_usu_externo: bool = BaseModel.BooleanField()
    ds_descripcion: str = BaseModel.CharField()
    ds_dipregep20: str = BaseModel.CharField()
    id: str = BaseModel.CharField()

    class Meta:
        table_name = 'ax_mod_revista'
        default_db = 'adabas'


@dataclass
class tmp_estadistica1y2(BaseModel):
    cd_encuadre_previs: int = BaseModel.IntegerField()
    cd_planta: int = BaseModel.IntegerField()
    cd_usuario_alta: str = BaseModel.CharField()
    dt_alta: datetime = BaseModel.DateTimeField()
    id_carga: int = BaseModel.IntegerField()
    id_cargo: int = BaseModel.IntegerField()
    nu_cuil: int = BaseModel.IntegerField()
    qt_aporte_personal: int = BaseModel.IntegerField()
    qt_cont_patronal: int = BaseModel.IntegerField()
    qt_nominal: int = BaseModel.IntegerField()

    class Meta:
        table_name = 'tmp_estadistica1y2'
        default_db = 'adabas'


@dataclass
class tmp_estadistica3y4(BaseModel):
    cd_concepto: str = BaseModel.CharField()
    cd_concepto_emplea: str = BaseModel.CharField()
    cd_usuario_alta: str = BaseModel.CharField()
    ds_concepto: str = BaseModel.CharField()
    dt_alta: datetime = BaseModel.DateTimeField()
    id_carga: int = BaseModel.IntegerField()
    id_cargo: int = BaseModel.IntegerField()
    nu_cuil: int = BaseModel.IntegerField()
    qt_importe: int = BaseModel.IntegerField()
    tp_concepto: str = BaseModel.CharField()

    class Meta:
        table_name = 'tmp_estadistica3y4'
        default_db = 'adabas'
