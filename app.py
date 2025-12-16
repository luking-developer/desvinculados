import streamlit as st
import polars as pl
import sqlite3
import pandas as pd # Necesario para st.data_editor
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
# 2. FUNCIONES DE LÓGICA DE NEGOCIO Y MANEJO DE ARCHIVOS
# ==============================================================================

def normalizar_fecha(fecha_str):
    """Convierte cadena de fecha abreviada al formato 'YYYY-MM-DD'."""
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
                normalized_date = datetime(int(year), month_num, int(day))
                return normalized_date.strftime(DATE_FORMAT)

        datetime.strptime(clean_str, DATE_FORMAT)
        return clean_str 

    except Exception:
        return None 

def cargar_db(uploaded_file):
    """Carga DB de disco a Polars DataFrame en memoria."""
    conn = None
    conn_disk = None
    temp_file_path = None
    
    db_bytes = uploaded_file.read()
    # 🚨 CAMBIO DE REQUISITO: Remover el almacenamiento de db_bytes 
    # para evitar la descarga del archivo original.
    
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
        st.success(f"Base de datos cargada con {len(df)} registros usando Polars.")
        
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
            st.session_state.data = df_combined.unique(subset=['nro_cli'], keep='first')
        else:
            st.session_state.data = df_csv.unique(subset=['nro_cli'], keep='first')

        st.success(f"CSV importado con Polars. Total de registros: {len(st.session_state.data)}.")

    except Exception as e:
        st.error(f"Error durante el procesamiento del CSV: {e}")

def guardar_db_bytes(df):
    """Convierte el Polars DataFrame a un archivo DB binario para descarga."""
    conn = sqlite3.connect(':memory:')
    
    # Escribir el DF de Polars a SQLite en memoria
    try:
        df.write_database(
            table_name='desvinculados', 
            connection=conn, 
            if_exists='replace',
            database_driver='sqlite' 
        )
    except Exception:
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
# 3. INTERFAZ DE USUARIO (STREAMLIT)
# ==============================================================================

st.set_page_config(layout="wide", page_title="Gestor EPE")
st.title("⚡ Gestor Web de Desvinculados EPE")

# 1. Inicialización del estado de sesión
if 'db_cargada' not in st.session_state:
    st.session_state.db_cargada = False
if 'data' not in st.session_state:
    st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)
    
# --- Controles de Carga ---
st.header("1. Carga de Datos")
col1, col2 = st.columns(2)

with col1:
    db_file = st.file_uploader("Cargar Base de Datos Existente (.db)", type=['db', 'sqlite'])
    if db_file: 
        cargar_db(db_file)
    # 🚨 BOTÓN DE DESCARGA ORIGINAL ELIMINADO SEGÚN REQUERIMIENTO DEL USUARIO

with col2:
    csv_file = st.file_uploader("Cargar Archivo CSV para Nuevos Registros", type=['csv'])
    if csv_file:
        procesar_csv(csv_file)
        
# --- Interfaz de Edición ---
if len(st.session_state.data) > 0:
    
    st.header(f"2. Edición de Registros ({len(st.session_state.data)} en memoria)")
    
    # Conversión CRÍTICA: Polars a Pandas
    df_edit_pandas = st.session_state.data.to_pandas()
    
    # CONVERSIÓN A DATETIME DE PANDAS (Necesario para el DateColumn)
    try:
        df_edit_pandas['fecha_intervencion'] = pd.to_datetime(
            df_edit_pandas['fecha_intervencion'], format=DATE_FORMAT, errors='coerce'
        )
        df_edit_pandas['fecha_alta'] = pd.to_datetime(
            df_edit_pandas['fecha_alta'], format=DATE_FORMAT, errors='coerce'
        )
        
    except Exception as e:
        st.error(f"Fallo grave al convertir columnas de fecha: {e}")
        
    # Llenar valores NaT (inválidos) con la fecha de hoy para la interfaz
    hoy_datetime = datetime.now().date() 
    df_edit_pandas['fecha_intervencion'] = df_edit_pandas['fecha_intervencion'].fillna(hoy_datetime)
    df_edit_pandas['fecha_alta'] = df_edit_pandas['fecha_alta'].fillna(hoy_datetime)
    

    estado_config = st.column_config.SelectboxColumn("Estado", options=list(ESTADOS), required=True)
    fecha_config = st.column_config.DateColumn("Fecha Intervención", format=DATE_FORMAT, required=True)

    edited_df_pandas = st.data_editor(
        df_edit_pandas,
        column_config={
            "estado": estado_config,
            "fecha_intervencion": fecha_config
        },
        disabled=('nro_cli', 'nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta'), 
        hide_index=True,
        key="data_editor_polars"
    )

    # 3. Guardar cambios y volver a Polars (CONVERSIÓN INVERSA)
    df_return_polars = pl.from_pandas(edited_df_pandas)
    
    st.session_state.data = df_return_polars \
        .with_columns(
            pl.col('fecha_intervencion').dt.strftime(DATE_FORMAT).alias('fecha_intervencion'),
            pl.col('fecha_alta').dt.strftime(DATE_FORMAT).alias('fecha_alta') 
        )

    st.header("3. Finalizar y Exportar")
    
    # 3.1 Descarga de Datos (Incluye el nuevo botón DB)

    st.download_button(
        label="💾 Descargar Base de Datos Actualizada (.db)",
        data=guardar_db_bytes(st.session_state.data),
        file_name='desvinculados_actualizado.db',
        mime='application/octet-stream',
        help="Guarda la base de datos actualizada con los cambios de edición y los registros CSV."
    )
    
    st.markdown("---") # Separador para el formato alternativo

    st.download_button(
        label="⬇️ Descargar CSV (Alternativo)",
        data=st.session_state.data.write_csv(None).encode('utf-8'),
        file_name='desvinculados_actualizado.csv',
        mime='text/csv',
        help="Descarga los datos en formato CSV (más compatible con entornos web)."
    )
    
    # 3.2 Envío por Email (Lógica Pendiente)
    '''st.markdown(f"""
    **🚨 Envío a Email:** El envío a `lzurverra@epe.santafe.gov.ar` requiere configuración SMTP y credenciales en variables de entorno de Render.
    """)'''
    
else:
    st.info("Por favor, cargue una Base de Datos existente o un CSV para comenzar a trabajar.")