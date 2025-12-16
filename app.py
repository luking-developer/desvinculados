import streamlit as st
import pandas as pd
import polars as pl
import sqlite3
import io
import os 
import re
import uuid 
from datetime import datetime

# ==============================================================================
# 1. CONFIGURACIÓN Y CONSTANTES
# ==============================================================================

DATE_FORMAT = '%Y-%m-%d'

# Mapeo de abreviaturas de meses (Ajustado para el ejemplo "oct.")
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

# Esquema estricto para forzar tipos de datos antes de la concatenación (Crítico para Polars)
FINAL_SCHEMA = {
    'nro_cli': pl.Int64,
    'nro_med': pl.Int64,
    'usuario': pl.Utf8,
    'domicilio': pl.Utf8,
    'normalizado': pl.Int64, # 0 o 1
    'fecha_alta': pl.Utf8, # YYYY-MM-DD
    'fecha_intervencion': pl.Utf8,
    'estado': pl.Utf8
}

# ==============================================================================
# 2. FUNCIONES DE LÓGICA DE NEGOCIO Y MANEJO DE ARCHIVOS
# ==============================================================================

def normalizar_fecha(fecha_str):
    """
    Convierte una cadena de fecha abreviada (ej. '3 oct. 2011') al formato 'YYYY-MM-DD'.
    Retorna None si el formato es irreconocible.
    """
    if fecha_str is None or pl.Series([fecha_str]).is_null().item():
        return datetime.now().strftime(DATE_FORMAT)

    fecha_str = str(fecha_str) 
    try:
        clean_str = fecha_str.lower().replace('.', '').strip()
        
        # Intentar parsear el formato '3 oct 2011'
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
        # Fallo de normalización o parsing
        return None 

def cargar_db(uploaded_file):
    """
    Carga una base de datos SQLite existente (uploaded_file) a un Polars 
    DataFrame en memoria, usando archivos temporales en /tmp para la transferencia.
    """
    conn = None
    conn_disk = None
    temp_file_path = None
    
    try:
        # 1. Crear ruta de archivo temporal única en /tmp
        temp_file_path = f"/tmp/{uuid.uuid4()}.db"
        
        # 2. Leer y escribir el contenido de la DB subida al disco temporal
        db_bytes = uploaded_file.read()
        with open(temp_file_path, "wb") as f:
            f.write(db_bytes)
            
        # 3. Conectar a la DB temporal en disco y a la DB en memoria
        conn_disk = sqlite3.connect(temp_file_path)
        conn = sqlite3.connect(':memory:')

        # 4. Transferir contenido de disco a memoria (MÉTODO ROBUSTO)
        conn_disk.backup(conn)
        
        # 5. Leer datos de la DB en memoria
        cursor = conn.cursor()
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
        st.error(f"Error al leer la tabla 'desvinculados'. El archivo podría estar corrupto: {e}")
        st.session_state.db_cargada = False
    except Exception as e:
        st.error(f"Error inesperado al cargar la DB: {e}")
        st.session_state.db_cargada = False
    finally:
        # 6. Limpieza crítica
        if conn_disk:
             conn_disk.close()
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)

def procesar_csv(uploaded_csv):
    """Procesa y normaliza el CSV, y lo fusiona con los datos existentes (Polars)."""
    try:
        # 1. Leer CSV
        df_csv = pl.read_csv(uploaded_csv, encoding='utf-8')
        
        # 2. Renombrar columnas
        df_csv = df_csv.rename({k: v for k, v in CSV_TO_DB_MAPPING.items()})
        
        # 3. Normalizar FECHA_ALTA y limpiar
        df_csv = df_csv.with_columns(
            pl.col('fecha_alta')
              .map_elements(normalizar_fecha, return_dtype=pl.Utf8)
              .alias('fecha_alta')
        ).filter(pl.col('fecha_alta').is_not_null()) # Elimina inválidos

        # 4. Normalizar NORMALIZADO (0/1)
        df_csv = df_csv.with_columns(
             pl.when(pl.col('normalizado').cast(pl.Utf8).str.to_lowercase().is_in(TRUE_VALUES))
               .then(pl.lit(1).cast(pl.Int64)) # Se asegura que sea Int64
               .otherwise(pl.lit(0).cast(pl.Int64))
               .alias('normalizado')
        )
        
        # 5. Establecer valores por defecto (PENDIENTE, fecha actual)
        hoy = datetime.now().strftime(DATE_FORMAT)
        df_csv = df_csv.with_columns([
            pl.lit('pendiente').alias('estado'),
            pl.lit(hoy).alias('fecha_intervencion')
        ])
        
        # 6. Seleccionar, reordenar y asegurar tipos de datos (CRÍTICO)
        df_csv = df_csv.select(
            [pl.col(col).cast(dtype) for col, dtype in FINAL_SCHEMA.items() if col in df_csv.columns]
        )

        # 7. Fusión
        if st.session_state.data is not None and len(st.session_state.data) > 0:
            
            # Asegurarse de que el DF existente también tenga el esquema correcto
            existing_df = st.session_state.data.select(
                [pl.col(col).cast(dtype) for col, dtype in FINAL_SCHEMA.items()]
            )
            
            # Concatenación vertical (Alineación por columnas y tipo)
            df_combined = pl.concat([existing_df, df_csv], how="vertical")
            
            # unique (Eliminar duplicados por nro_cli)
            st.session_state.data = df_combined.unique(subset=['nro_cli'], keep='first')
        else:
            st.session_state.data = df_csv.unique(subset=['nro_cli'], keep='first')

        st.success(f"CSV importado con Polars. Total de registros: {len(st.session_state.data)}.")

    except Exception as e:
        st.error(f"Error durante el procesamiento del CSV: {e}")

# ==============================================================================
# 3. INTERFAZ DE USUARIO (STREAMLIT)
# ==============================================================================

st.set_page_config(layout="wide", page_title="Gestor EPE")
st.title("⚡ Gestor Web de Desvinculados EPE")

# 1. Inicialización del estado de sesión
if 'db_cargada' not in st.session_state:
    st.session_state.db_cargada = False
if 'data' not in st.session_state:
    # Inicializa el DataFrame con el esquema vacío de Polars
    schema = {
        'nro_cli': pl.Int64, 'nro_med': pl.Int64, 'usuario': pl.Utf8, 
        'domicilio': pl.Utf8, 'normalizado': pl.Int64, 'fecha_alta': pl.Utf8, 
        'fecha_intervencion': pl.Utf8, 'estado': pl.Utf8
    }
    st.session_state.data = pl.DataFrame({}, schema=schema)
    
# --- Controles de Carga ---
st.header("1. Carga de Datos")
col1, col2 = st.columns(2)

with col1:
    db_file = st.file_uploader("Cargar Base de Datos Existente (.db)", type=['db', 'sqlite'])
    if db_file: # Se quita el 'and not st.session_state.db_cargada' para permitir recarga
        cargar_db(db_file)

with col2:
    csv_file = st.file_uploader("Cargar Archivo CSV para Nuevos Registros", type=['csv'])
    if csv_file:
        procesar_csv(csv_file)
        
# --- Interfaz de Edición ---
if len(st.session_state.data) > 0:
    
    st.header(f"2. Edición de Registros ({len(st.session_state.data)} en memoria)")
    
    # Conversión CRÍTICA: Polars a Pandas
    df_edit_pandas = st.session_state.data.to_pandas()
    
    # 🚨 CORRECCIÓN CRÍTICA: LIMPIEZA Y CONVERSIÓN A DATETIME DE PANDAS 🚨
    
    # 1. Limpiar y convertir 'fecha_intervencion' a datetime64[ns]
    try:
        # Forzar la conversión de la columna a datetime de Pandas, usando el formato estricto
        df_edit_pandas['fecha_intervencion'] = pd.to_datetime(
            df_edit_pandas['fecha_intervencion'], 
            format=DATE_FORMAT, 
            errors='coerce' # Convierte valores inválidos (como esos signos extraños) a NaT
        )
        
        # 2. Limpiar y convertir 'fecha_alta' a datetime64[ns] (solo fecha)
        # Esto asegura que si hay basura en la columna, se limpia a NaT
        df_edit_pandas['fecha_alta'] = pd.to_datetime(
            df_edit_pandas['fecha_alta'], 
            format=DATE_FORMAT, 
            errors='coerce' 
        )
        
    except Exception as e:
        # Este error es muy improbable aquí, pero lo capturamos
        st.error(f"Fallo grave al convertir columnas de fecha: {e}")
        
    # Llenar valores NaT (inválidos) con la fecha de hoy para evitar errores en la interfaz
    # Pandas no puede manejar NaT en el data_editor para columnas requeridas.
    hoy_datetime = datetime.now().date() # Usamos objeto date para ser estricto
    
    # Convertir NaT (Not a Time) a la fecha de hoy.
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
    # Volvemos a Polars, asegurando que la fecha se guarde SOLO como string YYYY-MM-DD (sin hora).
    
    # Paso A: Convertir el DataFrame editado de vuelta a Polars
    df_return_polars = pl.from_pandas(edited_df_pandas)
    
    # Paso B: Usar dt.strftime para forzar el formato YYYY-MM-DD (sin hora)
    st.session_state.data = df_return_polars \
        .with_columns(
            pl.col('fecha_intervencion').dt.strftime(DATE_FORMAT).alias('fecha_intervencion'),
            pl.col('fecha_alta').dt.strftime(DATE_FORMAT).alias('fecha_alta') 
        )
    # Aseguramos que la columna 'fecha_alta' permanece de tipo Utf8 (string) si no es un datetime.
    
    # 3.1 Descarga de Datos (Recomendado CSV para entorno efímero)
    st.markdown("⚠️ **Recomendación:** Descargue como CSV para evitar fallos de persistencia de archivos `.db` en Render Free.")

    st.download_button(
        label="💾 Descargar CSV Actualizado",
        data=st.session_state.data.write_csv(None).encode('utf-8'),
        file_name='desvinculados_actualizado.csv',
        mime='text/csv',
        help="Guarda los cambios en un archivo CSV."
    )

    # 3.2 Envío por Email (Lógica Pendiente)
    st.markdown(f"""
    **🚨 Envío a Email:** El envío de archivos adjuntos requiere configuración SMTP (usuario y contraseña) como variables de entorno de Render.
    
    **Si el email es crítico:** Necesitas una función separada que use `smtplib` y las variables de entorno para enviar el archivo `.csv` (o `.db`) a `lzurverra@epe.santafe.gov.ar`.
    """, unsafe_allow_html=True)
    
else:
    st.info("Por favor, cargue una Base de Datos existente o un CSV para comenzar a trabajar.")