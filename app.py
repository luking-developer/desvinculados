import streamlit as st
import polars as pl
import sqlite3
import pandas as pd
import io
import os 
import re
import tempfile
import uuid 
import zipfile
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

# Mapeo específico para archivos ODS (Columna X)
ODS_ESTADO_MAP = {
    '+': 'cargado',
    '?': 'revisar',
    'x': 'otro distrito',
    '-': 'otro distrito',
    '': 'pendiente'
}

TRUE_VALUES = {'1', 't', 'true', 'si', 's'} 
ESTADOS = ('cargado', 'pendiente', 'revisar', 'otro distrito')
FILTRO_OPTIONS = ["Todos los registros"] + list(ESTADOS) # Opciones de filtro de vista

class ColumnMapper:
    def __init__(self, mapping: dict):
        self._csv_to_db = mapping
        self._db_to_csv = {v: k for k, v in mapping.items()}

    def get_db_col(self, csv_col: str) -> str:
        return self._csv_to_db.get(csv_col, f"MISSING_{csv_col}")

    def get_csv_col(self, db_col: str) -> str:
        return self._db_to_csv.get(db_col, f"MISSING_{db_col}")

CSV_TO_DB_MAPPING = {
    'NROCLI': 'nro_cli', 
    'NUMERO_MEDIDOR': 'nro_med', 
    'FULLNAME': 'usuario',
    'DOMICILIO_COMERCIAL': 'domicilio', 
    'NORMALIZADO': 'normalizado', 
    'FECHA_ALTA': 'fecha_alta',
    'FECHA_INTERVENCION': 'fecha_intervencion',
    'ESTADO': 'estado'
}

mapper = ColumnMapper(CSV_TO_DB_MAPPING)

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

ODS_SCHEMA = {
    'X': pl.Utf8,
    'NRO CLI': pl.Int64,
    'NRO MED': pl.Int64,
    'USUARIO': pl.Utf8,
    'DOMICILIO': pl.Utf8,
    'N': pl.Int64,
    'FECHA INTERVENCION': pl.Utf8
}

# ==============================================================================
# 2. FUNCIONES DE LÓGICA
# ==============================================================================

def normalizar_fecha(fecha_str):
    if fecha_str is None or str(fecha_str).lower() in ['none', 'nan', '']:
        return datetime.now().strftime(DATE_FORMAT)
    fs = str(fecha_str).lower().replace('.', '').strip()
    try:
        match = re.match(r'(\d{1,2})\s*([a-z]+)\s*(\d{4})', fs)
        if match:
            d, m_abbr, y = match.groups()
            m_num = MONTH_MAPPING.get(m_abbr)
            if m_num: return f"{y}-{m_num:02d}-{int(d):02d}"
        return fs 
    except:
        return datetime.now().strftime(DATE_FORMAT)

def procesar_archivo_inteligente(uploaded_file):
    try:
        raw_content = uploaded_file.getvalue()
        nombre = uploaded_file.name.lower()
        hoy = datetime.now().strftime(DATE_FORMAT)
        
        if nombre.endswith('.csv'):
            df = pl.read_csv(io.BytesIO(raw_content), infer_schema_length=10000)
            if 'NORMALIZADO' in df.columns:
                df = df.with_columns(pl.col('NORMALIZADO').cast(pl.Utf8).str.to_lowercase().is_in(TRUE_VALUES).cast(pl.Int64))
            df = df.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df.columns})
            
        elif nombre.endswith('.ods'):
            with io.BytesIO(raw_content) as bio:
                pd_df = pd.read_excel(bio, engine='odf')
                df = pl.from_pandas(pd_df)
            
            primera_col = df.columns[0]
            if primera_col.upper() == 'X':
                df = df.with_columns(
                    pl.col(primera_col).cast(pl.Utf8).fill_null('').str.strip_chars().str.to_lowercase()
                    .map_elements(lambda x: ODS_ESTADO_MAP.get(x, 'pendiente'), return_dtype=pl.Utf8)
                    .alias('estado')
                ).drop(primera_col)
                df = df.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df.columns})

        # --- Lógica de Estado (REQUERIMIENTO: Copiar si existe, sino 'pendiente') ---
        if 'estado' not in df.columns:
            df = df.with_columns(pl.lit('pendiente').alias('estado'))
        else:
            df = df.with_columns(pl.col('estado').fill_null('pendiente'))

        if 'fecha_intervencion' not in df.columns:
            df = df.with_columns(pl.lit(hoy).alias('fecha_intervencion'))

        if 'fecha_alta' in df.columns:
            df = df.with_columns(pl.col('fecha_alta').map_elements(normalizar_fecha, return_dtype=pl.Utf8))

        # Asegurar esquema
        for col, dtype in FINAL_SCHEMA.items():
            if col not in df.columns: df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
        
        df = df.select([pl.col(c).cast(FINAL_SCHEMA[c]) for c in FINAL_SCHEMA.keys()])

        if len(st.session_state.data) > 0:
            st.session_state.data = pl.concat([st.session_state.data, df], how="vertical").unique(subset=['nro_cli'], keep='last')
        else:
            st.session_state.data = df
        st.success(f"Procesado: {len(df)} filas.")
        st.rerun()
    except Exception as e:
        st.error(f"Error: {e}")

def exportar_todo(df):
    # 1. DB SQLite
    conn_mem = sqlite3.connect(':memory:')
    df.to_pandas().to_sql('desvinculados', conn_mem, if_exists='replace', index=False)
    tmp_path = f"/tmp/{uuid.uuid4()}.db"
    with sqlite3.connect(tmp_path) as disk_conn: conn_mem.backup(disk_conn)
    with open(tmp_path, "rb") as f: db_bytes = f.read()
    os.remove(tmp_path)

    # 2. CSV con Mapper
    df_csv = df.clone()
    rename_map = {col: mapper.get_csv_col(col) for col in df_csv.columns if not mapper.get_csv_col(col).startswith("MISSING_")}
    csv_bytes = df_csv.rename(rename_map).write_csv().encode('utf-8')

    # 3. ZIP
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        zf.writestr("desvinculados.db", db_bytes)
        zf.writestr("desvinculados.csv", csv_bytes)
    return zip_buf.getvalue()

def cargar_db(uploaded_file):
    """Carga DB de disco a Polars DataFrame en memoria."""
    conn = None
    conn_disk = None
    temp_file_path = None
    
    db_bytes = uploaded_file.read()
    
    try:
        temp_path = f"/tmp/{uuid.uuid4()}.db"
        with open(temp_path, "wb") as f: f.write(uploaded_file.getvalue())
        conn = sqlite3.connect(temp_path)
        st.session_state.data = pl.read_database("SELECT * FROM desvinculados", conn)
        conn.close()
        os.remove(temp_path)
        st.success("Base de datos cargada.")
    except Exception as e: st.error(f"Error DB: {e}")

# ==============================================================================
# 3. INTERFAZ
# ==============================================================================

st.set_page_config(layout="wide", page_title="Gestor EPE")
if 'data' not in st.session_state: st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)

st.title("⚡ EPE - Gestión de Desvinculados")

t1, t2 = st.tabs(["📊 Gestión", "⚙️ Sistema"])

with t1:
    c1, c2 = st.columns(2)
    with c1:
        f_db = st.file_uploader("📂 Cargar DB", type=['db', 'sqlite'])
        if f_db: cargar_db(f_db)
    with c2:
        f_in = st.file_uploader("📥 Importar CSV/ODS", type=['csv', 'ods'])
        if f_in: procesar_archivo_inteligente(f_in)

    if len(st.session_state.data) > 0:
        st.divider()
        filtro = st.selectbox("Vista:", FILTRO_OPTIONS, index=2)
        df_v = st.session_state.data.clone()
        if filtro != "Todos los registros": df_v = df_v.filter(pl.col('estado') == filtro)

        pdf = df_v.to_pandas()
        for c in ['fecha_intervencion', 'fecha_alta']: pdf[c] = pd.to_datetime(pdf[c], errors='coerce').dt.date
        
        edited = st.data_editor(pdf, column_config={
            "estado": st.column_config.SelectboxColumn("estado", options=ESTADOS, required=True),
            # "fecha_intervencion": st.column_config.DateColumn("fecha_intervencion", format="YYYY-MM-DD")
            "fecha_intervencion": st.column_config.DateColumn("fecha_intervencion", format="DD-MM-YYYY")
        }, disabled=('nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta'), num_rows='dynamic', hide_index=True)

        if st.button("💾 Aplicar cambios"):
            res = pl.from_pandas(edited).with_columns([pl.col(c).cast(pl.Utf8) for c in ['fecha_intervencion', 'fecha_alta']])
            st.session_state.data = pl.concat([st.session_state.data.filter(~pl.col('nro_cli').is_in(res['nro_cli'])), res], how="vertical").filter(pl.col('nro_cli').is_not_null())
            st.success("Guardado.")
            st.rerun()

        st.divider()
        # col_ex, col_cl = st.columns([2, 1])
        col_ex = st.columns(1)
        with col_ex:
            st.download_button("📦 Descargar todo (DB + CSV)", data=exportar_todo(st.session_state.data), file_name="desvinculados_epe.zip", mime="application/zip", use_container_width=True)
        # with col_cl:
        #     if st.button("🗑️ Limpiar 'cargado'", type="primary", use_container_width=True):
        #         st.session_state.data = st.session_state.data.filter(pl.col('estado') != 'cargado')
        #         st.rerun()

with t2:
    st.info("Sistema Operativo.")
