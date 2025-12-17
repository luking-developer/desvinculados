import streamlit as st
import polars as pl
import sqlite3
import pandas as pd
import io
import os 
import re
import uuid 
from datetime import datetime

# ==============================================================================
# 1. CONFIGURACIÓN Y CONSTANTES
# ==============================================================================

DATE_FORMAT = '%Y-%m-%d'

MONTH_MAPPING = {
    'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4,
    'may': 5, 'jun': 6, 'jul': 7, 'ago': 8,
    'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dic': 12
}

# Expandido para evitar errores de conversión str -> i64
TRUE_VALUES = {'1', 't', 'true', 'si', 's', 'x', '+'} 
ESTADOS = ('cargado', 'pendiente', 'revisar', 'otro distrito')
FILTRO_OPTIONS = ["Todos los registros"] + list(ESTADOS)

CSV_TO_DB_MAPPING = {
    'NROCLI': 'nro_cli', 'NUMERO_MEDIDOR': 'nro_med', 'FULLNAME': 'usuario',
    'DOMICILIO_COMERCIAL': 'domicilio', 'NORMALIZADO': 'normalizado', 'FECHA_ALTA': 'fecha_alta'
}

# El esquema final siempre debe ser consistente
FINAL_SCHEMA = {
    'nro_cli': pl.Int64,
    'nro_med': pl.Int64,
    'usuario': pl.Utf8,
    'domicilio': pl.Utf8,
    'normalizado': pl.Int64, 
    'fecha_alta': pl.Utf8, 
    'fecha_intervencion': pl.Utf8,
    'estado': pl.Utf8
}

# ==============================================================================
# 2. FUNCIONES DE LÓGICA DE NEGOCIO
# ==============================================================================

def normalizar_fecha(fecha_str):
    if fecha_str is None or str(fecha_str).strip() == "" or str(fecha_str) == "None":
        return datetime.now().strftime(DATE_FORMAT)
    fecha_str = str(fecha_str) 
    try:
        clean_str = fecha_str.lower().replace('.', '').strip()
        match = re.match(r'(\d{1,2})\s*([a-z]+)\s*(\d{4})', clean_str)
        if match:
            day, month_abbr, year = match.groups()
            month_num = MONTH_MAPPING.get(month_abbr, None)
            if month_num:
                return datetime(int(year), month_num, int(day)).strftime(DATE_FORMAT)
        return clean_str 
    except:
        return datetime.now().strftime(DATE_FORMAT)

def mapear_estado_x(simbolo):
    """Lógica específica para la columna 'X'"""
    simbolo = str(simbolo).strip().lower() if simbolo else ""
    if simbolo == '+': return 'cargado'
    if simbolo == '?': return 'revisar'
    if simbolo == 'x': return 'otro distrito'
    return 'pendiente'

def limpiar_normalizado(serie):
    """Evita el error de conversion str a i64 manejando SI/NO"""
    return serie.cast(pl.Utf8).str.to_lowercase().map_elements(
        lambda x: 1 if x in TRUE_VALUES else 0, return_dtype=pl.Int64
    )

def fusionar_datos(df_nuevo):
    """Une los datos nuevos con los de la sesión evitando duplicados por nro_cli"""
    if st.session_state.data is not None and len(st.session_state.data) > 0:
        existing_df = st.session_state.data.select([pl.col(c).cast(FINAL_SCHEMA[c]) for c in FINAL_SCHEMA])
        df_combined = pl.concat([existing_df, df_nuevo], how="vertical")
        st.session_state.data = df_combined.unique(subset=['nro_cli'], keep='first')
    else:
        st.session_state.data = df_nuevo

def procesar_archivo_especial(file):
    """Maneja la importación con la columna 'X' como estado (CSV/ODS)"""
    try:
        # Intentar leer según extensión
        if file.name.endswith('.ods'):
            df_raw = pl.from_pandas(pd.read_excel(file, engine='odf'))
        else:
            df_raw = pl.read_csv(file, infer_schema_length=10000)

        # Verificar si la primera columna es "X"
        if df_raw.columns[0].upper() != 'X':
            st.error("El archivo no tiene la columna 'X' como primera columna.")
            return

        # Renombrar columnas según mapeo
        df = df_raw.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df_raw.columns})
        
        # Lógica de Estado basada en columna X
        col_x = df_raw.columns[0]
        df = df.with_columns([
            pl.col(col_x).map_elements(mapear_estado_x, return_dtype=pl.Utf8).alias('estado'),
            pl.lit(datetime.now().strftime(DATE_FORMAT)).alias('fecha_intervencion')
        ])

        # Limpiar 'normalizado' para evitar el error i64
        if 'normalizado' in df.columns:
            df = df.with_columns(limpiar_normalizado(pl.col('normalizado')).alias('normalizado'))

        # Normalizar fechas y asegurar esquema
        df = df.with_columns(pl.col('fecha_alta').map_elements(normalizar_fecha, return_dtype=pl.Utf8))
        df = df.select([pl.col(c).cast(FINAL_SCHEMA[c]) for c in FINAL_SCHEMA if c in df.columns])
        
        fusionar_datos(df)
        st.success(f"Importación especial completada: {len(df)} registros.")
    except Exception as e:
        st.error(f"Error crítico en importación: {e}")

def cargar_db(uploaded_file):
    try:
        temp_path = f"/tmp/{uuid.uuid4()}.db"
        with open(temp_path, "wb") as f: f.write(uploaded_file.read())
        conn_disk = sqlite3.connect(temp_path)
        conn_mem = sqlite3.connect(':memory:')
        conn_disk.backup(conn_mem)
        df = pl.read_database("SELECT * FROM desvinculados", conn_mem)
        st.session_state.data = df.with_columns(pl.col('fecha_intervencion').fill_null(datetime.now().strftime(DATE_FORMAT)))
        st.success("Base de datos cargada.")
        os.remove(temp_path)
    except Exception as e:
        st.error(f"Error al cargar DB: {e}")

def guardar_db_bytes(df):
    conn = sqlite3.connect(':memory:')
    df.to_pandas().to_sql('desvinculados', conn, if_exists='replace', index=False)
    temp_path = f"/tmp/{uuid.uuid4()}.db"
    conn_disk = sqlite3.connect(temp_path)
    conn.backup(conn_disk)
    conn_disk.close()
    with open(temp_path, "rb") as f: bytes_data = f.read()
    os.remove(temp_path)
    return bytes_data

# ==============================================================================
# 3. INTERFAZ DE USUARIO
# ==============================================================================

st.set_page_config(layout="wide", page_title="Gestor EPE")

# Inicializar sesión
if 'data' not in st.session_state:
    st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)

# --- NAVEGACIÓN SUPERIOR ---
tab_gestion, tab_importar = st.tabs(["📊 Gestión de Registros", "📥 Importar hoja de cálculo"])

with tab_importar:
    st.header("Importación Especial (Columna X)")
    st.info("Sube un CSV u ODS donde la 1ra columna sea 'X' (+, ?, x) para definir el estado.")
    archivo_esp = st.file_uploader("Seleccionar archivo", type=['csv', 'ods'], key="uploader_esp")
    if archivo_esp:
        procesar_archivo_especial(archivo_esp)

with tab_gestion:
    st.header("1. Carga de datos base")
    c1, c2 = st.columns(2)
    with c1:
        db_f = st.file_uploader("Cargar .db existente", type=['db', 'sqlite'])
        if db_f: cargar_db(db_f)
    with c2:
        csv_f = st.file_uploader("Cargar CSV estándar", type=['csv'])
        if csv_f:
            # Reutilizamos la lógica de limpieza de normalizado aquí también para evitar el error i64
            try:
                df_c = pl.read_csv(csv_f).rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in pl.read_csv(csv_f).columns})
                if 'normalizado' in df_c.columns:
                    df_c = df_c.with_columns(limpiar_normalizado(pl.col('normalizado')))
                df_c = df_c.with_columns([
                    pl.lit('pendiente').alias('estado'),
                    pl.lit(datetime.now().strftime(DATE_FORMAT)).alias('fecha_intervencion'),
                    pl.col('fecha_alta').map_elements(normalizar_fecha, return_dtype=pl.Utf8)
                ])
                fusionar_datos(df_c.select([pl.col(c).cast(FINAL_SCHEMA[c]) for c in FINAL_SCHEMA]))
                st.success("CSV estándar cargado.")
            except Exception as e: st.error(f"Error CSV: {e}")

    if len(st.session_state.data) > 0:
        st.divider()
        st.header(f"2. Edición ({len(st.session_state.data)} registros)")
        
        f_est = st.selectbox("Filtrar por estado:", FILTRO_OPTIONS, index=1)
        df_view = st.session_state.data.clone()
        if f_est != "Todos los registros":
            df_view = df_view.filter(pl.col('estado') == f_est)

        df_pd = df_view.to_pandas()
        for c in ['fecha_intervencion', 'fecha_alta']:
            df_pd[c] = pd.to_datetime(df_pd[c], errors='coerce').dt.date

        edited_pd = st.data_editor(
            df_pd,
            column_config={
                "estado": st.column_config.SelectboxColumn("estado", options=ESTADOS, required=True),
                "fecha_intervencion": st.column_config.DateColumn("fecha_intervencion", format="YYYY-MM-DD")
            },
            disabled=('nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta'),
            num_rows='dynamic', hide_index=True
        )

        if st.button("✅ Guardar cambios"):
            new_pl = pl.from_pandas(edited_pd).filter(pl.col('nro_cli').is_not_null())
            new_pl = new_pl.with_columns([
                pl.col('fecha_intervencion').cast(pl.Utf8),
                pl.col('fecha_alta').cast(pl.Utf8)
            ])
            ids = new_pl.get_column('nro_cli')
            unaffected = st.session_state.data.filter(~pl.col('nro_cli').is_in(ids))
            st.session_state.data = pl.concat([unaffected, new_pl.select(st.session_state.data.columns)], how="vertical")
            st.success("Cambios en memoria guardados.")
            st.rerun()

        st.divider()
        st.header("3. Finalizar y exportar")
        if st.button("🗑️ Eliminar registros 'cargado'"):
            st.session_state.data = st.session_state.data.filter(pl.col('estado') != 'cargado')
            st.rerun()

        cf1, cf2 = st.columns(2)
        with cf1:
            st.download_button("💾 Descargar .db", data=guardar_db_bytes(st.session_state.data), file_name="gestion_epe.db")
        with cf2:
            st.download_button("⬇️ Descargar .csv", data=st.session_state.data.write_csv().encode('utf-8'), file_name="gestion_epe.csv")
    else:
        st.info("Sin datos para mostrar.")