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

# Mapeo robusto para el campo normalizado
TRUE_VALUES = {'1', 't', 'true', 'si', 's', 'yes'} 

# Mapeo para archivos ODS (Columna 'X')
ODS_ESTADO_MAP = {
    "+": "cargado",
    "?": "revisar",
    "": "pendiente",
    None: "pendiente",
    "x": "otro distrito",
    "-": "otro distrito"
}

ESTADOS = ('cargado', 'pendiente', 'revisar', 'otro distrito')
FILTRO_OPTIONS = ["Todos los registros"] + list(ESTADOS)

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
    if fecha_str is None or str(fecha_str).strip() in ["", "None", "nan"]:
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

        return pd.to_datetime(clean_str).strftime(DATE_FORMAT)
    except:
        return datetime.now().strftime(DATE_FORMAT)

def normalizar_booleanos(serie):
    """Convierte SI/NO, 1/0 o True/False a entero 1/0 de forma segura."""
    return serie.cast(pl.Utf8).str.to_lowercase().is_in(TRUE_VALUES).cast(pl.Int64)

def cargar_db(uploaded_file):
    try:
        db_bytes = uploaded_file.read()
        temp_file_path = f"/tmp/{uuid.uuid4()}.db"
        with open(temp_file_path, "wb") as f:
            f.write(db_bytes)
            
        conn_disk = sqlite3.connect(temp_file_path)
        conn = sqlite3.connect(':memory:')
        conn_disk.backup(conn)
        
        df = pl.read_database("SELECT * FROM desvinculados", conn)
        df = df.with_columns(pl.col('fecha_intervencion').fill_null(datetime.now().strftime(DATE_FORMAT)))
        
        st.session_state.data = df
        st.session_state.db_cargada = True
        st.success(f"Base de datos cargada: {len(df)} registros.")
        conn_disk.close()
        os.remove(temp_file_path)
    except Exception as e:
        st.error(f"Error cargando DB: {e}")

def procesar_csv(uploaded_csv):
    """Procesamiento de CSV con corrección de error str -> i64 en 'normalizado'."""
    try:
        # Leemos todo como string inicialmente para evitar el error de casteo automático
        df_csv = pl.read_csv(uploaded_csv, infer_schema_length=0)
        df_csv = df_csv.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df_csv.columns})
        
        # Aplicar correcciones de tipos
        df_csv = df_csv.with_columns([
            pl.col('nro_cli').cast(pl.Int64),
            pl.col('nro_med').cast(pl.Int64),
            normalizar_booleanos(pl.col('normalizado')).alias('normalizado'),
            pl.col('fecha_alta').map_elements(normalizar_fecha, return_dtype=pl.Utf8)
        ])
        
        hoy = datetime.now().strftime(DATE_FORMAT)
        df_csv = df_csv.with_columns([
            pl.lit('pendiente').alias('estado'),
            pl.lit(hoy).alias('fecha_intervencion')
        ])

        fusionar_datos(df_csv)
    except Exception as e:
        st.error(f"Error crítico en CSV: {e}")

def procesar_ods(uploaded_ods):
    """Lógica para importar ODS con mapeo especial de columna 'X'."""
    try:
        # Polars no lee ODS nativamente de forma directa tan fácil, usamos pandas como puente
        pdf = pd.read_excel(uploaded_ods, engine='odf')
        
        if pdf.columns[0] != 'X':
            st.error("Archivo Inválido: La primera columna debe llamarse 'X'")
            return

        df_ods = pl.from_pandas(pdf)
        
        # Mapeo de la columna X al estado
        df_ods = df_ods.with_columns([
            pl.col('X').map_elements(lambda x: ODS_ESTADO_MAP.get(str(x).strip() if x else "", "pendiente"), return_dtype=pl.Utf8).alias('estado')
        ])

        # Renombrar columnas restantes según el mapeo estándar
        df_ods = df_ods.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df_ods.columns})
        
        # Normalización de tipos
        df_ods = df_ods.with_columns([
            pl.col('nro_cli').cast(pl.Int64),
            pl.col('nro_med').cast(pl.Int64),
            normalizar_booleanos(pl.col('normalizado')).alias('normalizado'),
            pl.col('fecha_alta').map_elements(normalizar_fecha, return_dtype=pl.Utf8),
            pl.lit(datetime.now().strftime(DATE_FORMAT)).alias('fecha_intervencion')
        ])

        fusionar_datos(df_ods)
        st.success("Hoja de cálculo ODS importada correctamente.")
    except Exception as e:
        st.error(f"Error procesando ODS: {e}")

def fusionar_datos(nuevo_df):
    """Une los nuevos datos con los existentes en memoria evitando duplicados."""
    nuevo_df = nuevo_df.select([pl.col(col).cast(dtype) for col, dtype in FINAL_SCHEMA.items()])
    
    if len(st.session_state.data) > 0:
        combined = pl.concat([st.session_state.data, nuevo_df], how="vertical")
        st.session_state.data = combined.unique(subset=['nro_cli'], keep='first')
    else:
        st.session_state.data = nuevo_df
    st.rerun()

def guardar_db_bytes(df):
    conn = sqlite3.connect(':memory:')
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
# 3. INTERFAZ DE USUARIO (STREAMLIT)
# ==============================================================================

st.set_page_config(layout="wide", page_title="Gestor EPE", page_icon="⚡")

if 'data' not in st.session_state:
    st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)

# --- MENÚ DE NAVEGACIÓN SUPERIOR ---
menu = st.tabs(["📊 Gestión Principal", "📥 Importar hoja de cálculo", "⚙️ Configuración"])

# --- TAB 1: GESTIÓN PRINCIPAL ---
with menu[0]:
    st.title("⚡ Panel de Gestión EPE")
    
    col_u1, col_u2 = st.columns(2)
    with col_u1:
        db_file = st.file_uploader("Cargar Base .db", type=['db', 'sqlite'], key="main_db")
        if db_file: cargar_db(db_file)
    with col_u2:
        csv_file = st.file_uploader("Cargar registros .csv", type=['csv'], key="main_csv")
        if csv_file: procesar_csv(csv_file)

    if len(st.session_state.data) > 0:
        st.divider()
        
        # Filtros y ABM
        filtro_estado = st.selectbox("Filtrar vista por estado:", FILTRO_OPTIONS, index=1) # Default: pendiente
        
        df_view = st.session_state.data.clone()
        if filtro_estado != "Todos los registros":
            df_view = df_view.filter(pl.col('estado') == filtro_estado)

        # Editor de datos
        df_edit_pandas = df_view.to_pandas()
        for col in ['fecha_intervencion', 'fecha_alta']:
            df_edit_pandas[col] = pd.to_datetime(df_edit_pandas[col], errors='coerce').dt.date

        edited_df = st.data_editor(
            df_edit_pandas,
            column_config={
                "estado": st.column_config.SelectboxColumn("Estado", options=ESTADOS, required=True),
                "fecha_intervencion": st.column_config.DateColumn("Intervención"),
                "normalizado": st.column_config.CheckboxColumn("Normalizado")
            },
            disabled=('nro_med', 'usuario', 'domicilio', 'fecha_alta'),
            num_rows='dynamic',
            hide_index=True,
            key="main_editor"
        )

        if st.button("✅ Guardar cambios en memoria", use_container_width=True):
            df_save = pl.from_pandas(edited_df)
            df_save = df_save.with_columns([
                pl.col('fecha_intervencion').dt.strftime(DATE_FORMAT),
                pl.col('fecha_alta').dt.strftime(DATE_FORMAT),
                pl.col('nro_cli').cast(pl.Int64)
            ]).filter(pl.col('nro_cli') > 0)
            
            # Merge lógico
            unaffected = st.session_state.data.filter(~pl.col('nro_cli').is_in(df_save['nro_cli']))
            st.session_state.data = pl.concat([unaffected, df_save], how="vertical")
            st.success("Memoria actualizada.")
            st.rerun()

        # Exportación
        st.header("3. Finalizar y exportar")
        if st.button("🗑️ Limpiar registros 'cargado'"):
            st.session_state.data = st.session_state.data.filter(pl.col('estado') != 'cargado')
            st.rerun()

        c1, c2 = st.columns(2)
        c1.download_button("💾 Descargar .db", guardar_db_bytes(st.session_state.data), "base_actualizada.db", use_container_width=True)
        c2.download_button("⬇️ Descargar .csv", st.session_state.data.write_csv(), "datos.csv", use_container_width=True)

# --- TAB 2: IMPORTAR HOJA DE CÁLCULO (ODS) ---
with menu[1]:
    st.header("📥 Importar Hoja de Cálculo (Formato ODS)")
    st.info("""
    **Instrucciones del formato:**
    1. La primera columna debe llamarse **'X'**.
    2. Valores aceptados en 'X': `+` (cargado), `?` (revisar), `-` / `x` (otro distrito), vacío (pendiente).
    """)
    
    ods_file = st.file_uploader("Seleccione archivo .ods", type=['ods'])
    if ods_file:
        if st.button("🚀 Procesar e Importar ODS"):
            procesar_ods(ods_file)

# --- TAB 3: CONFIGURACIÓN ---
with menu[2]:
    st.subheader("Estado de la Aplicación")
    st.write(f"Registros en memoria: {len(st.session_state.data)}")
    if st.button("🔥 Reset Total (Borrar memoria)"):
        st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)
        st.rerun()