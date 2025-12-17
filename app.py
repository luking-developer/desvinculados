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
ESTADOS = ('cargado', 'pendiente', 'revisar', 'otro distrito')
FILTRO_OPTIONS = ["Todos los registros"] + list(ESTADOS)

# Mapeo específico para la columna "X" del ODS
MAPEO_ESTADO_X = {
    "+": "cargado",
    "?": "revisar",
    "x": "otro distrito",
    None: "pendiente",
    "": "pendiente"
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

def fusionar_datos(df_nuevo):
    """Lógica central para unir datos nuevos con lo que ya existe en sesión."""
    if st.session_state.data is not None and len(st.session_state.data) > 0:
        existing_df = st.session_state.data.select([pl.col(c).cast(t) for c, t in FINAL_SCHEMA.items()])
        df_combined = pl.concat([existing_df, df_nuevo], how="vertical")
        st.session_state.data = df_combined.unique(subset=['nro_cli'], keep='first')
    else:
        st.session_state.data = df_nuevo
    st.success(f"Datos integrados. Total actual: {len(st.session_state.data)} registros.")

def procesar_ods(uploaded_ods):
    """Procesa archivo ODS con lógica de columna 'X' para el estado."""
    try:
        # Leemos con Pandas (motor odf) y pasamos a Polars
        pdf = pd.read_excel(uploaded_ods, engine='odf')
        df = pl.from_pandas(pdf)
        
        # 1. Renombrar columnas según el estándar
        df = df.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df.columns})
        
        # 2. Lógica de la columna "X" para determinar el estado
        if "X" in df.columns:
            df = df.with_columns(
                pl.col("X").map_elements(lambda x: MAPEO_ESTADO_X.get(str(x).strip() if x else "", "pendiente"), return_dtype=pl.Utf8).alias("estado")
            )
        else:
            df = df.with_columns(pl.lit("pendiente").alias("estado"))

        # 3. Normalizar resto de campos
        hoy = datetime.now().strftime(DATE_FORMAT)
        df = df.with_columns([
            pl.col('fecha_alta').map_elements(normalizar_fecha, return_dtype=pl.Utf8),
            pl.lit(hoy).alias('fecha_intervencion'),
            pl.when(pl.col('normalizado').cast(pl.Utf8).str.to_lowercase().is_in(TRUE_VALUES))
              .then(pl.lit(1).cast(pl.Int64)).otherwise(pl.lit(0).cast(pl.Int64)).alias('normalizado')
        ]).filter(pl.col('fecha_alta').is_not_null())

        # Asegurar esquema y fusionar
        df = df.select([pl.col(col).cast(dtype) for col, dtype in FINAL_SCHEMA.items() if col in df.columns])
        fusionar_datos(df)
    except Exception as e:
        st.error(f"Error procesando ODS: {e}")

def cargar_db(uploaded_file):
    """Carga DB de disco a Polars DataFrame en memoria."""
    conn = None
    conn_disk = None
    temp_file_path = None
    
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
        
        if not data:
             st.warning("La tabla 'desvinculados' estaba vacía.")
             schema = {col: pl.Utf8 for col in column_names}
             df = pl.DataFrame({col: [] for col in column_names}, schema=schema)
        else:
             df = pl.DataFrame(data, schema=column_names)

        df = df.with_columns(
            pl.col('fecha_intervencion').fill_null(datetime.now().strftime(DATE_FORMAT))
        )
        
        st.session_state.data = df
        st.session_state.db_cargada = True
        st.success(f"Base de datos cargada con {len(df)} registros.")
        
    except sqlite3.OperationalError as e:
        st.error(f"Error al leer la tabla 'desvinculados'. El archivo podría estar corrupto: {e}")
        st.session_state.db_cargada = False
    except Exception as e:
        st.error(f"Error inesperado al cargar la DB: {e}")
        st.session_state.db_cargada = False
    finally:
        if conn_disk:
             conn_disk.close()
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)

def procesar_csv(uploaded_csv):
    """Procesa y fusiona el CSV con los datos existentes (Polars)."""
    try:
        df_csv = pl.read_csv(uploaded_csv, encoding='utf-8')
        df_csv = df_csv.rename({k: v for k, v in CSV_TO_DB_MAPPING.items()})
        
        # Normalización de Fechas y Valores por defecto
        df_csv = df_csv.with_columns(
            pl.col('fecha_alta')
              .map_elements(normalizar_fecha, return_dtype=pl.Utf8)
              .alias('fecha_alta')
        ).filter(pl.col('fecha_alta').is_not_null())
        
        df_csv = df_csv.with_columns(
             pl.when(pl.col('normalizado').cast(pl.Utf8).str.to_lowercase().is_in(TRUE_VALUES))
               .then(pl.lit(1).cast(pl.Int64))
               .otherwise(pl.lit(0).cast(pl.Int64))
               .alias('normalizado')
        )
        
        hoy = datetime.now().strftime(DATE_FORMAT)
        df_csv = df_csv.with_columns([
            pl.lit('pendiente').alias('estado'),
            pl.lit(hoy).alias('fecha_intervencion')
        ])
        
        # Asegurar esquema (CRÍTICO)
        df_csv = df_csv.select(
            [pl.col(col).cast(dtype) for col, dtype in FINAL_SCHEMA.items() if col in df_csv.columns]
        )

        # Fusión
        if st.session_state.data is not None and len(st.session_state.data) > 0:
            existing_df = st.session_state.data.select(
                [pl.col(col).cast(dtype) for col, dtype in FINAL_SCHEMA.items()]
            )
            df_combined = pl.concat([existing_df, df_csv], how="vertical")
            # Usar unique para desduplicar por nro_cli
            st.session_state.data = df_combined.unique(subset=['nro_cli'], keep='first')
        else:
            st.session_state.data = df_csv.unique(subset=['nro_cli'], keep='first')

        st.success(f"CSV importado. Total de registros: {len(st.session_state.data)}.")

    except Exception as e:
        st.error(f"Error durante el procesamiento del CSV: {e}")

def guardar_db_bytes(df):
    """Convierte el Polars DataFrame a un archivo DB binario para descarga."""
    conn = sqlite3.connect(':memory:')
    
    # Escribir el DF de Polars a SQLite en memoria
    try:
        # Intentar con Polars writer (más eficiente)
        df.write_database(
            table_name='desvinculados', 
            connection=conn, 
            if_exists='replace',
            database_driver='sqlite' 
        )
    except Exception:
        # Fallback a Pandas si el writer de Polars falla
        df.to_pandas().to_sql('desvinculados', conn, if_exists='replace', index=False)
        
    # Transferir de DB en memoria a archivo binario para la descarga
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

# --- NAVEGACIÓN ---
with st.sidebar:
    st.title("Navegación")
    menu = st.radio("Ir a:", ["Panel Principal", "Importar hoja de cálculo"])

# 1. Inicialización de sesión
if 'data' not in st.session_state:
    st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)

# --- VISTA: IMPORTAR HOJA DE CÁLCULO ---
if menu == "Importar hoja de cálculo":
    st.header("Importar datos desde ODS")
    st.info("Esta opción procesa la columna 'X' para determinar el estado del registro (+, ?, x).")
    ods_file = st.file_uploader("Subir archivo .ods", type=['ods'])
    if ods_file:
        procesar_ods(ods_file)
    if st.button("Volver al Panel"):
        st.rerun()

# --- VISTA: PANEL PRINCIPAL ---
else:
    st.title("⚡ Gestor Web de Desvinculados EPE")
    
    # 1. Carga de datos (DB y CSV original)
    st.header("1. Carga de datos")
    col1, col2 = st.columns(2)
    with col1:
        db_file = st.file_uploader("Cargar Base de Datos (.db)", type=['db', 'sqlite'])
        if db_file: cargar_db()
    with col2:
        csv_file = st.file_uploader("Cargar CSV (estándar)", type=['csv'])
        if csv_file: procesar_csv()

    # 2. Gestión de registros (ABM + Filtro)
    if len(st.session_state.data) > 0:
        # ... [Insertar aquí toda la lógica de edición, filtro y botón 'Guardar cambios' del código base] ...
        
        # 3. Finalizar y exportar
        st.header("3. Finalizar y exportar")
        
        # Botón Limpiar 'cargado'
        if st.button("🗑️ Eliminar registros con estado 'cargado'"):
            st.session_state.data = st.session_state.data.filter(pl.col('estado') != 'cargado')
            st.rerun()

        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            st.download_button("💾 Descargar .db", data=guardar_db_bytes(st.session_state.data), file_name='datos.db')
        with c2:
            st.download_button("⬇️ Descargar .csv", data=st.session_state.data.write_csv(None).encode('utf-8'), file_name='datos.csv')
    else:
        st.info("Sin datos. Use la carga inicial o el menú lateral para importar un ODS.")