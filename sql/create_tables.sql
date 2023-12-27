DROP TABLE IF EXISTS files;
CREATE TABLE files (
    file_name TEXT,
    uploaded_at timestamp,
    file_hash TEXT UNIQUE NOT NULL,
    ended BOOLEAN DEFAULT FALSE
);
DROP TABLE IF EXISTS salario;
CREATE TABLE "salario" (
    "id" bigserial NOT NULL PRIMARY KEY,
    "is_loaded" BOOLEAN DEFAULT FALSE,
    "nu_cuil" bigint NOT NULL,
    "cd_empleador" varchar(10) NOT NULL,
    "nu_cargo" bigint NOT NULL,
    "periodo_liq" date NOT NULL,
    "nu_liquidacion" bigint NOT NULL,
    "tp_liquidacion" varchar(10) NOT NULL,
    "tp_ddjj" varchar(10) NOT NULL,
    "cd_concepto_ips" varchar(10) NOT NULL,
    "cd_concepto_emp" varchar(10) NOT NULL,
    "tp_concepto" varchar(10) NOT NULL,
    "ds_concepto" varchar(100) NOT NULL,
    "qt_importe_a_signo" varchar(1) NOT NULL,
    "qt_importe" bigint NOT NULL,
    "nu_anios_antiguedad_ips" bigint NOT NULL,
    "nu_meses_antiguedad_ips" bigint NOT NULL,
    "nu_anios_antiguedad_otras" bigint NOT NULL,
    "nu_meses_antiguedad_otras" bigint NOT NULL,
    "nu_anios_antiguedad_doc" bigint NOT NULL,
    "nu_meses_antiguedad_doc" bigint NOT NULL,
    "cd_forma_pago" varchar(10) NOT NULL,
    "nu_inasistencia_con_desc" bigint NOT NULL,
    "nu_lic_sin_sueldo" bigint NOT NULL,
    "cd_encuadre_previs" varchar(10) NOT NULL,
    "cd_modalidad_revista" varchar(10) NOT NULL,
    "file_id" TEXT NULL
);
DROP TABLE IF EXISTS foja;
CREATE TABLE "foja" (
    cd_empleador varchar(8) not null,
    cd_motivo_alta bigint,
    cd_motivo_baja bigint,
    cd_subvencionado boolean default false,
    cd_usuario_actual char(10),
    cd_usuario_alta char(10),
    ds_clave_prov varchar(10),
    ds_escalafon_vig varchar(50),
    ds_observacion varchar(250),
    dt_acto_alta date,
    dt_acto_baja date,
    dt_actual timestamp,
    dt_alta timestamp,
    dt_ingreso date not null,
    id_cargo bigserial NOT NULL PRIMARY KEY,
    id_certificacion bigint,
    nu_acto_alta bigint,
    nu_acto_baja bigint,
    nu_cargo bigint,
    nu_cue bigint,
    nu_cuil bigint not null,
    nu_legajo varchar(20),
    tp_acto_alta bigint,
    tp_acto_baja bigint
);