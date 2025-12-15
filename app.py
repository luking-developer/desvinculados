import streamlit as st
import polars as pl # ¡CAMBIO CRÍTICO!
import sqlite3
import io
import os 
import re
from datetime import datetime

# Definiciones y Mapeos (Se mantienen)
DATE_FORMAT = '%Y-%m-%d'
MONTH_MAPPING = {
    'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4,
    'may': 5, 'jun': 6, 'jul': 7, 'ago': 8,
    'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dic': 12
}
TRUE_VALUES = {'1', 't', 'true', 'si', 's'} 
DB_COLUMNS = ['nro_cli', 'nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta', 'fecha_intervencion', 'estado']
CSV_TO_DB_MAPPING = {
    'NROCLI': 'nro_cli', 'NUMERO_MEDIDOR': 'nro_med', 'FULLNAME': 'usuario',
    'DOMICILIO_COMERCIAL': 'domicilio', 'NORMALIZADO': 'normalizado', 'FECHA_ALTA': 'fecha_alta'
}
ESTADOS = ('cargado', 'pendiente', 'revisar', 'otro distrito')

# --- Funciones de Lógica de Negocio (Adaptadas para Polars) ---

def normalizar_fecha(fecha_str):
    """
    Convierte una cadena de fecha abreviada al formato 'YYYY-MM-DD'.
    Esta función debe ser estricta para funcionar con map_elements de Polars.
    """
    if not fecha_str or pl.Series([fecha_str]).is_null().item():
        return datetime.now().strftime(DATE_FORMAT)

    fecha_str = str(fecha_str) # Asegurar que es string
    try:
        clean_str = fecha_str.lower().replace('.', '').strip()
        match = re.match(r'(\d{1,2})\s*([a-z]+)\s*(\d{4})', clean_str)
        
        if match:
            day, month_abbr, year = match.groups()
            month_num = MONTH_MAPPING.get(month_abbr, None)
            
            if month_num is not None:
                normalized_date = datetime(int(year), month_num, int(day))
                return normalized_date.strftime(DATE_FORMAT)

        # Intentar parsear si ya está en formato estándar YYYY-MM-DD
        datetime.strptime(clean_str, DATE_FORMAT)
        return clean_str 

    except Exception:
        # Si la normalización o el parsing fallan
        return None 

def cargar_db(uploaded_file):
    """Carga una base de datos SQLite existente a un Polars DataFrame en memoria."""
    # Uso temporal de io.BytesIO para manejar el archivo subido
    db_bytes = uploaded_file.read()
    conn = sqlite3.connect(':memory:')
    
    # Cargar datos del archivo subido en la conexión en memoria
    temp_conn = sqlite3.connect(io.BytesIO(db_bytes))
    
    try:
        # Leer datos con la librería estándar y luego crear DataFrame de Polars
        cursor = temp_conn.cursor()
        cursor.execute("SELECT * FROM desvinculados")
        data = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        
        if not data:
             st.warning("La tabla 'desvinculados' estaba vacía.")
             df = pl.DataFrame({col: [] for col in column_names})
        else:
             df = pl.DataFrame(data, schema=column_names)

        # Rellenar fecha_intervencion faltante
        df = df.with_columns(
            pl.col('fecha_intervencion').fill_null(datetime.now().strftime(DATE_FORMAT))
        )
        
        st.session_state.data = df
        st.session_state.db_cargada = True
        st.success(f"Base de datos cargada con {len(df)} registros usando Polars.")
        
    except sqlite3.OperationalError as e:
        st.error(f"Error al leer la tabla 'desvinculados' del archivo DB. Archivo corrupto o esquema incorrecto: {e}")
        st.session_state.db_cargada = False
    finally:
        temp_conn.close()


def procesar_csv(uploaded_csv):
    """Procesa y normaliza el CSV, y lo fusiona con los datos existentes (Polars)."""
    try:
        # 1. Leer CSV directamente con Polars
        # Usamos cursor.read_csv para manejar el archivo subido de Streamlit
        df_csv = pl.read_csv(uploaded_csv, encoding='utf-8')
        
        # 2. Renombrar columnas
        df_csv = df_csv.rename({k: v for k, v in CSV_TO_DB_MAPPING.items()})
        
        # 3. Normalizar FECHA_ALTA y limpiar (Polars `map_elements`)
        df_csv = df_csv.with_columns(
            pl.col('fecha_alta')
              .map_elements(normalizar_fecha, return_dtype=pl.Utf8)
              .alias('fecha_alta')
        ).filter(pl.col('fecha_alta').is_not_null()) # Filtra registros con fecha inválida

        # 4. Normalizar NORMALIZADO (Booleano 0/1)
        # Usamos una expresión condicional para la conversión a 0/1
        df_csv = df_csv.with_columns(
             pl.when(pl.col('normalizado').cast(pl.Utf8).str.to_lowercase().is_in(TRUE_VALUES))
               .then(pl.lit(1))
               .otherwise(pl.lit(0))
               .alias('normalizado')
        )
        
        # 5. Establecer valores por defecto para nuevos registros
        hoy = datetime.now().strftime(DATE_FORMAT)
        df_csv = df_csv.with_columns([
            pl.lit('pendiente').alias('estado'),
            pl.lit(hoy).alias('fecha_intervencion')
        ])
        
        # 6. Seleccionar y reordenar columnas
        cols_to_keep = [col for col in DB_COLUMNS if col in df_csv.columns]
        df_csv = df_csv.select(cols_to_keep)

        # 7. Fusión (CRÍTICO: Polars concat y unique)
        if 'data' in st.session_state and st.session_state.data is not None and len(st.session_state.data) > 0:
            df_combined = pl.concat([st.session_state.data, df_csv])
            # drop_duplicates en Polars se llama unique
            st.session_state.data = df_combined.unique(subset=['nro_cli'], keep='first')
        else:
            st.session_state.data = df_csv

        st.success(f"CSV importado con Polars. Total de registros: {len(st.session_state.data)}.")

    except Exception as e:
        st.error(f"Error durante el procesamiento del CSV con Polars: {e}")

def guardar_db(df):
    """Guarda el Polars DataFrame modificado a un archivo DB para descarga."""
    
    # 1. Crear conexión temporal para escritura
    conn = sqlite3.connect(':memory:')
    
    # 2. Escribir el DataFrame de Polars a SQLite
    # Convertimos Polars a Pandas solo para usar el to_sql robusto, 
    # o usamos el método write_database de Polars (más nativo)
    
    # Opción 1: Usar write_database de Polars (preferida)
    try:
        df.write_database(
            table_name='desvinculados', 
            connection=conn, 
            if_exists='replace',
            database_driver='sqlite' # Importante para Polars
        )
    except Exception:
        # Fallback: Convertir a Pandas si write_database da problemas
        df.to_pandas().to_sql('desvinculados', conn, if_exists='replace', index=False)

    # 3. Leer los bytes de la DB para el download_button
    buffer = io.BytesIO()
    
    # Usamos la conexión para escribir la DB a un objeto binario
    # Tienes que copiar el contenido de la DB en memoria a un archivo físico (BytesIO)
    with conn:
        for line in conn.iterdump():
            if line not in ('BEGIN;', 'COMMIT;'):
                buffer.write(f'{line}\n'.encode('utf-8'))