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

TRUE_VALUES = {'1', 't', 'true', 'si', 's'} 
DB_COLUMNS = ['nro_cli', 'nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta', 'fecha_intervencion', 'estado']
ESTADOS = ('cargado', 'pendiente', 'revisar', 'otro distrito')
FILTRO_OPTIONS = ["Todos los registros"] + list(ESTADOS)

# Mapeo específico para el archivo tipo "X"
X_SYMBOL_MAPPING = {
    '+': 'cargado',
    '?': 'revisar',
    'x': 'otro distrito',
    '': 'pendiente',
    None: 'pendiente'
}

CSV_TO_DB_MAPPING = {
    'NROCLI': 'nro_cli', 'NUMERO_MEDIDOR': 'nro_med', 'FULLNAME': 'usuario',
    'DOMICILIO_COMERCIAL': 'domicilio', 'NORMALIZADO': 'normalizado', 'FECHA_ALTA': 'fecha_alta'
}

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
    if fecha_str is None or pl.Series([fecha_str]).is_null().item():
        return datetime.now().strftime(DATE_FORMAT)
    fecha_str = str(fecha_str) 
    try:
        clean_str = fecha_str.lower().replace('.', '').strip()
        match = re.match(r'(\d{1,2})\s*([a-z]+)\s*(\d{4})', clean_str)
        if match:
            day, month_abbr, year = match.groups()
            month_num = MONTH_MAPPING.get(month_abbr, None)
            if month_num is not None:
                return datetime(int(year), month_num, int(day)).strftime(DATE_FORMAT)
        datetime.strptime(clean_str, DATE_FORMAT)
        return clean_str 
    except Exception:
        return None 

def cargar_db(uploaded_file):
    db_bytes = uploaded_file.read()
    try:
        temp_file_path = f"/tmp/{uuid.uuid4()}.db"
        with open(temp_file_path, "wb") as f:
            f.write(db_bytes)
        conn_disk = sqlite3.connect(temp_file_path)
        conn = sqlite3.connect(':memory:')
        conn_disk.backup(conn)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM desvinculados")
        data = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        df = pl.DataFrame(data, schema=column_names) if data else pl.DataFrame({}, schema={c: pl.Utf8 for c in column_names})
        st.session_state.data = df.with_columns(pl.col('fecha_intervencion').fill_null(datetime.now().strftime(DATE_FORMAT)))
        st.session_state.db_cargada = True
        st.success(f"Base de datos cargada: {len(st.session_state.data)} registros.")
    except Exception as e:
        st.error(f"Error al cargar DB: {e}")
    finally:
        if 'conn_disk' in locals(): conn_disk.close()
        if os.path.exists(temp_file_path): os.remove(temp_file_path)

def procesar_csv(uploaded_csv):
    """
    Procesa el CSV detectando si es un formato estándar o formato especial 'X'.
    """
    try:
        df_raw = pl.read_csv(uploaded_csv, encoding='utf-8', infer_schema_length=10000)
        
        # --- DETECCIÓN DE FORMATO ESPECIAL "X" ---
        if df_raw.columns[0] == "X":
            st.info("Detectado formato especial con columna de estado 'X'.")
            
            # 1. Mapear símbolos de la columna X a estados legibles
            # Usamos replace para mayor velocidad en Polars
            df_csv = df_raw.with_columns(
                pl.col("X").replace(X_SYMBOL_MAPPING, default="pendiente").alias("estado")
            )
            
            # 2. Renombrar el resto de columnas según el mapeo estándar (si aplica)
            df_csv = df_csv.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df_csv.columns})
            
        else:
            # --- FORMATO ESTÁNDAR ---
            st.info("Detectado formato estándar de registros nuevos.")
            df_csv = df_raw.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df_raw.columns})
            
            # En formato estándar, todos entran como 'pendiente'
            hoy = datetime.now().strftime(DATE_FORMAT)
            df_csv = df_csv.with_columns([
                pl.lit('pendiente').alias('estado'),
                pl.lit(hoy).alias('fecha_intervencion')
            ])

        # --- NORMALIZACIÓN COMÚN ---
        # Normalizar fechas de alta
        if 'fecha_alta' in df_csv.columns:
            df_csv = df_csv.with_columns(
                pl.col('fecha_alta').map_elements(normalizar_fecha, return_dtype=pl.Utf8)
            ).filter(pl.col('fecha_alta').is_not_null())
        
        # Normalizar columna 'normalizado' a 0/1
        if 'normalizado' in df_csv.columns:
            df_csv = df_csv.with_columns(
                 pl.when(pl.col('normalizado').cast(pl.Utf8).str.to_lowercase().is_in(TRUE_VALUES))
                   .then(pl.lit(1).cast(pl.Int64))
                   .otherwise(pl.lit(0).cast(pl.Int64))
            )

        # Asegurar que todas las columnas del esquema final existan (rellenar con null si faltan)
        for col, dtype in FINAL_SCHEMA.items():
            if col not in df_csv.columns:
                df_csv = df_csv.with_columns(pl.lit(None).cast(dtype).alias(col))

        # Seleccionar y castear columnas finales
        df_csv = df_csv.select([pl.col(col).cast(dtype) for col, dtype in FINAL_SCHEMA.items()])

        # Fusión con datos existentes
        if len(st.session_state.data) > 0:
            existing_df = st.session_state.data.select([pl.col(col).cast(dtype) for col, dtype in FINAL_SCHEMA.items()])
            df_combined = pl.concat([existing_df, df_csv], how="vertical")
            st.session_state.data = df_combined.unique(subset=['nro_cli'], keep='last') # 'last' para que el CSV pise a la DB si hay duplicados
        else:
            st.session_state.data = df_csv.unique(subset=['nro_cli'], keep='first')

        st.success(f"Importación finalizada. Total registros: {len(st.session_state.data)}.")
        st.rerun()

    except Exception as e:
        st.error(f"Error procesando CSV: {e}")

def guardar_db_bytes(df):
    conn = sqlite3.connect(':memory:')
    try:
        df.write_database(table_name='desvinculados', connection=conn, if_exists='replace', database_driver='sqlite')
    except Exception:
        df.to_pandas().to_sql('desvinculados', conn, if_exists='replace', index=False)
    temp_file_path = f"/tmp/{uuid.uuid4()}.db"
    conn_disk = sqlite3.connect(temp_file_path)
    conn.backup(conn_disk)
    conn_disk.close()
    with open(temp_file_path, "rb") as f:
        db_bytes = f.read()
    os.remove(temp_file_path)
    return db_bytes

# ==============================================================================
# 3. INTERFAZ DE USUARIO
# ==============================================================================

st.set_page_config(layout="wide", page_title="Gestor EPE")
st.title("⚡ Gestor Web de Desvinculados EPE")

if 'data' not in st.session_state:
    st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)

# --- Sección 1: Carga ---
st.header("1. Carga de datos")
col1, col2 = st.columns(2)
with col1:
    db_file = st.file_uploader("Cargar Base de Datos (.db)", type=['db', 'sqlite'])
    if db_file: cargar_db(db_file)
with col2:
    csv_file = st.file_uploader("Cargar CSV (Normal o Formato 'X')", type=['csv'])
    if csv_file: procesar_csv(csv_file)

# --- Sección 2: Gestión ---
if len(st.session_state.data) > 0:
    st.header(f"2. Gestión de registros ({len(st.session_state.data)} en memoria)")
    
    filtro_estado = st.selectbox("Filtrar registros:", options=FILTRO_OPTIONS, index=FILTRO_OPTIONS.index('pendiente'))
    df_to_edit = st.session_state.data.clone()
    if filtro_estado != "Todos los registros":
        df_to_edit = df_to_edit.filter(pl.col('estado') == filtro_estado)

    df_edit_pandas = df_to_edit.to_pandas()
    for col in ['fecha_intervencion', 'fecha_alta']:
        df_edit_pandas[col] = pd.to_datetime(df_edit_pandas[col], format=DATE_FORMAT, errors='coerce').fillna(datetime.now().date())

    edited_df_pandas = st.data_editor(
        df_edit_pandas,
        column_config={
            "estado": st.column_config.SelectboxColumn("estado", options=list(ESTADOS), required=True),
            "fecha_intervencion": st.column_config.DateColumn("fecha_intervencion", format=DATE_FORMAT, required=True)
        },
        disabled=('nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta'),
        num_rows='dynamic', 
        hide_index=True,
        key="editor"
    )
    
    if st.button("✅ Guardar cambios"):
        df_comm = pl.from_pandas(edited_df_pandas).filter((pl.col('nro_cli').is_not_null()) & (pl.col('nro_cli') > 0))
        df_comm = df_comm.with_columns([pl.col('fecha_intervencion').dt.strftime(DATE_FORMAT), pl.col('fecha_alta').dt.strftime(DATE_FORMAT)])
        nro_cli_comm = df_comm.get_column('nro_cli')
        df_unaffected = st.session_state.data.filter(~pl.col('nro_cli').is_in(nro_cli_comm))
        st.session_state.data = pl.concat([df_unaffected, df_comm], how="vertical")
        st.success("Cambios guardados.")
        st.rerun()

    # --- Sección 3: Exportar ---
    st.header("3. Finalizar y exportar")
    
    if st.button("🗑️ Eliminar registros 'cargado'"):
        st.session_state.data = st.session_state.data.filter(pl.col('estado') != 'cargado')
        st.rerun()

    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("💾 Descargar DB (.db)", data=guardar_db_bytes(st.session_state.data), file_name='desvinculados.db')
    with c2:
        st.download_button("⬇️ Descargar CSV", data=st.session_state.data.write_csv().encode('utf-8'), file_name='desvinculados.csv')

else:
    st.info("Inicie cargando una DB o un archivo CSV.")