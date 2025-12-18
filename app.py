import streamlit as st
import polars as pl
import sqlite3
import pandas as pd
import io
import os 
import re
import tempfile
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

# Mapeo específico para archivos ODS (Columna X)
ODS_ESTADO_MAP = {
    '+': 'cargado',
    '?': 'revisar',
    'x': 'otro distrito',
    '-': 'otro distrito',
    None: 'pendiente',
    '': 'pendiente'
}

TRUE_VALUES = {'1', 't', 'true', 'si', 's'} 
DB_COLUMNS = ['nro_cli', 'nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta', 'fecha_intervencion', 'estado']
ESTADOS = ('cargado', 'pendiente', 'revisar', 'otro distrito')
FILTRO_OPTIONS = ["Todos los registros"] + list(ESTADOS) # Opciones de filtro de vista

CSV_TO_DB_MAPPING = {
    'NROCLI': 'nro_cli', 'NUMERO_MEDIDOR': 'nro_med', 'FULLNAME': 'usuario',
    'DOMICILIO_COMERCIAL': 'domicilio', 'NORMALIZADO': 'normalizado', 'FECHA_ALTA': 'fecha_alta'
}

FINAL_SCHEMA = {
    'nro_cli': pl.Int64, # Clave primaria para el merge
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
        st.success(f"Base de datos cargada con {len(df)} {'registro' if len(df) == 1 else 'registros'}.")
        
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

def procesar_ods(uploaded_file):
    """
    Procesa archivos ODS usando un archivo temporal para evitar errores de puntero.
    """
    temp_path = None
    try:
        # Creamos archivo temporal físico
        with tempfile.NamedTemporaryFile(delete=False, suffix=".ods") as tmp:
            tmp.write(uploaded_file.getvalue())
            temp_path = tmp.name

        # LEER USANDO CALAMINE (Mucho más robusto que ODFPY)
        # Nota: Requiere 'pip install python-calamine'
        pd_df = pd.read_excel(temp_path, engine='calamine')
        df = pl.from_pandas(pd_df)
        
        if df.is_empty(): return None

        col_x = df.columns[0]
        if col_x.upper() != 'X':
            st.error(f"Error: Primera columna debe ser 'X', se leyó '{col_x}'")
            return None

        # Mapeo de estados
        df = df.with_columns(
            pl.col(col_x).cast(pl.Utf8).fill_null("")
            .map_elements(lambda s: ODS_ESTADO_MAP.get(s.strip().lower(), "pendiente"), return_dtype=pl.Utf8)
            .alias("estado")
        ).drop(col_x)

        # Renombrado y esquema
        df = df.rename({k: v for k, v in CSV_TO_DB_MAPPING.items() if k in df.columns})
        
        for col, dtype in FINAL_SCHEMA.items():
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
        
        return df.select([pl.col(c).cast(FINAL_SCHEMA[c]) for c in FINAL_SCHEMA.keys()])

    except Exception as e:
        st.error(f"Error en motor de lectura: {e}")
        return None
    finally:
        if temp_path and os.path.exists(temp_path): os.remove(temp_path)

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
# 3. INTERFAZ DE USUARIO (STREAMLIT)
# ==============================================================================

st.set_page_config(layout="wide", page_title="Desvinculados", page_icon="⚡")
st.title("⚡ Gestor Web de Desvinculados EPE")

# 1. Inicialización del estado de sesión
if 'db_cargada' not in st.session_state:
    st.session_state.db_cargada = False
if 'data' not in st.session_state:
    st.session_state.data = pl.DataFrame({}, schema=FINAL_SCHEMA)
    
# --- Controles de Carga ---
st.header("1. Carga de datos")
col1, col2 = st.columns(2)

with col1:
    db_file = st.file_uploader("Cargar Base de Datos existente (.db)", type=['db', 'sqlite'])
    if db_file: 
        cargar_db(db_file)

with col2:
    csv_ods_file = st.file_uploader("Importar hoja de cálculo (CSV u ODS) con registros", type=['csv', 'ods'])
    if csv_ods_file:
        nombre_archivo = csv_ods_file.name.lower()
        df_nuevo = procesar_csv(csv_ods_file) if nombre_archivo.endswith('.csv') else procesar_ods(csv_ods_file)
        
        if df_nuevo is not None:
            if len(st.session_state.data) > 0:
                st.session_state.data = pl.concat([st.session_state.data, df_nuevo], how="vertical").unique(subset=['nro_cli'], keep='last')
            else:
                st.session_state.data = df_nuevo
            st.success(f"Cargados {len(df_nuevo)} registros.")
            st.rerun()

# --- Interfaz de ABM y Edición ---
if len(st.session_state.data) > 0:
    
    st.header(f"2. Gestión de registros ({len(st.session_state.data)} en memoria)")

    # 🚨 NUEVO: Selector de Filtro 
    filtro_estado = st.selectbox(
        "Filtrar registros:",
        options=FILTRO_OPTIONS,
        index=FILTRO_OPTIONS.index('pendiente'), # Por defecto 'pendiente'
        key="filter_selectbox"
    )

    # Aplicar el filtro al DataFrame antes de pasarlo al editor
    df_to_edit = st.session_state.data.clone()
    
    if filtro_estado != "Todos los registros":
        df_to_edit = df_to_edit.filter(pl.col('estado') == filtro_estado)

    st.info(f"Mostrando {len(df_to_edit)} de {len(st.session_state.data)} registros en total.")

    # Conversión CRÍTICA: Polars (string dates) a Pandas (datetime dates)
    df_edit_pandas = df_to_edit.to_pandas()
    
    # Aplicar conversión de tipo a datetime para que el DateColumn de Streamlit funcione
    try:
        df_edit_pandas['fecha_intervencion'] = pd.to_datetime(
            df_edit_pandas['fecha_intervencion'], format=DATE_FORMAT, errors='coerce'
        )
        df_edit_pandas['fecha_alta'] = pd.to_datetime(
            df_edit_pandas['fecha_alta'], format=DATE_FORMAT, errors='coerce'
        )
        
    except Exception as e:
        st.error(f"Fallo grave al convertir columnas de fecha: {e}")
        
    # Llenar valores NaT (inválidos o nulos) con la fecha de hoy para la interfaz
    hoy_datetime = datetime.now().date() 
    df_edit_pandas['fecha_intervencion'] = df_edit_pandas['fecha_intervencion'].fillna(hoy_datetime)
    df_edit_pandas['fecha_alta'] = df_edit_pandas['fecha_alta'].fillna(hoy_datetime)
    
    # Configuración de columnas
    estado_config = st.column_config.SelectboxColumn("estado", options=list(ESTADOS), required=True)
    fecha_config = st.column_config.DateColumn("fecha_intervencion", format=DATE_FORMAT, required=True)

    # 🚨 ABM Interface: num_rows='dynamic' habilita Alta (Add row) y Baja (trash icon)
    # nro_cli ahora es editable para permitir el ABM
    edited_df_pandas = st.data_editor(
        df_edit_pandas,
        column_config={
            "estado": estado_config,
            "fecha_intervencion": fecha_config
        },
        disabled=('nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta'), # nro_cli removido de disabled
        num_rows='dynamic', 
        hide_index=True,
        key="data_editor_polars",
        placeholder=""
    )
    
    # 🚨 Botón de Guardado Explícito (Commit)
    if st.button("✅ Guardar cambios"):
        
        st.info("Procesando cambios...")
        
        # 1. Convertir el DF editado a Polars y limpiar tipos
        df_committed_polars = pl.from_pandas(edited_df_pandas)
        
        # Eliminar registros con nro_cli nulo o cero (nuevas filas que el usuario no llenó)
        df_committed_polars = df_committed_polars.filter(
            (pl.col('nro_cli').is_not_null()) & (pl.col('nro_cli') > 0)
        )
        
        # Aplicar la conversión inversa de fecha (datetime -> string YYYY-MM-DD)
        df_committed_polars = df_committed_polars \
            .with_columns(
                pl.col('fecha_intervencion').dt.strftime(DATE_FORMAT).alias('fecha_intervencion'),
                pl.col('fecha_alta').dt.strftime(DATE_FORMAT).alias('fecha_alta') 
            )

        # 2. Lógica de Fusión (Merge) para actualizar el DataFrame completo
        df_full_original = st.session_state.data.clone()
        
        # Obtener los nro_cli que estaban visibles/editados/añadidos
        nro_cli_committed = df_committed_polars.get_column('nro_cli')
        
        # Anti-Join: Mantener solo los registros originales que NO fueron afectados (no estaban visibles)
        # Esto elimina de la base original tanto los registros modificados como los eliminados (Baja) del subset visible
        df_unaffected = df_full_original.filter(
            ~pl.col('nro_cli').is_in(nro_cli_committed)
        )
        
        # 3. Concatenar los registros no afectados con los nuevos registros comprometidos (Modificación + Alta)
        st.session_state.data = pl.concat([df_unaffected, df_committed_polars], how="vertical")
        
        st.success(f"Cambios aplicados. Total de registros en memoria: {len(st.session_state.data)}.")
        
        # Forzar un nuevo render para actualizar el editor, el contador de registros y el filtro
        st.rerun() 

    # 3.1 Descarga de Datos (DB y CSV)

    st.header("3. Finalizar y exportar")
    
    col1_footer, col2_footer = st.columns(2)

    with col1_footer:
        st.download_button(
            label="💾 Descargar Base de Datos",
            data=guardar_db_bytes(st.session_state.data),
            file_name='desvinculados_actualizado.db',
            mime='application/octet-stream',
            help="Guarda la base de datos actualizada con los cambios de edición y los registros CSV.",
            width="stretch",
            shortcut="Ctrl+D"
        )
    
    #st.markdown("---")

    with col2_footer:
        st.download_button(
            label="⬇️ Descargar CSV",
            data=st.session_state.data.write_csv(None).encode('utf-8'),
            file_name='desvinculados_actualizado.csv',
            mime='text/csv',
            help="Descarga los datos en formato CSV.",
            width="stretch",
            shortcut="Ctrl+S"
        )
    
    # 3.2 Envío por Email (Lógica Pendiente)
    #st.markdown(f"""
    #**🚨 Envío a Email:** El envío a `lzurverra@epe.santafe.gov.ar` requiere configuración SMTP y credenciales en variables de entorno de Render.
    #""")
    
else:
    st.info("Por favor, cargue una Base de Datos existente, o un CSV u ODS para comenzar a trabajar.")