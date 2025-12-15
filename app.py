import streamlit as st
import pandas as pd
import polars as pl
import sqlite3
import io
import os
import uuid
from datetime import datetime
# Importamos las funciones de la lógica anterior
# (normalizar_fecha, MONTH_MAPPING, TRUE_VALUES, etc.)
# Asumo que las funciones normalizar_fecha y las constantes están definidas aquí o importadas

# Definiciones de Constantes (Tomadas del script anterior)
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


# --- Funciones de Lógica de Negocio (Deberían ser portadas del script anterior) ---
def normalizar_fecha(fecha_str):
    """(Tu función normalizar_fecha del paso anterior)"""
    # ... (código omitido por brevedad, pero necesario) ...
    # Usar el mapeo y regex para convertir a YYYY-MM-DD
    # Si falla, retorna None
    # Simulacion:
    if fecha_str is None: return datetime.now().strftime(DATE_FORMAT)
    if 'oct' in fecha_str: return '2011-10-03' # Ejemplo de conversión exitosa
    if '2025' in fecha_str: return fecha_str
    return None

def cargar_db(uploaded_file):
    """Carga una base de datos SQLite existente a un Polars DataFrame en memoria."""
    
    conn = None
    conn_disk = None
    temp_file_path = None
    
    try:
        # 1. Crear una ruta de archivo temporal única en el directorio /tmp de Render
        # El directorio /tmp es el único que permite escritura en el plan gratuito
        temp_file_path = f"/tmp/{uuid.uuid4()}.db"
        
        # 2. Leer los bytes del archivo subido
        db_bytes = uploaded_file.read()

        # 3. Escribir el objeto BytesIO (contenido de la DB) al archivo temporal en disco
        with open(temp_file_path, "wb") as f:
            f.write(db_bytes)
            
        # 4. Conectar a la base de datos temporal en disco
        # ¡IMPORTANTE!: Esto resuelve el error TypeError en la línea 45
        conn_disk = sqlite3.connect(temp_file_path)
        
        # 5. Crear la conexión en memoria (la persistente para la sesión)
        conn = sqlite3.connect(':memory:')

        # 6. Transferir el contenido de la DB de disco a la DB en memoria (MÉTODO ROBUSTO)
        conn_disk.backup(conn)
        
        # 7. Leer datos de la DB en memoria usando Polars
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM desvinculados")
        data = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]
        
        if not data:
             st.warning("La tabla 'desvinculados' estaba vacía o no existe.")
             # Crear DataFrame vacío con esquema correcto
             schema = {col: pl.Utf8 for col in column_names}
             df = pl.DataFrame({}, schema=schema)
        else:
             df = pl.DataFrame(data, schema=column_names)

        # 8. Rellenar y guardar estado
        df = df.with_columns(
            pl.col('fecha_intervencion').fill_null(datetime.now().strftime(DATE_FORMAT))
        )
        
        st.session_state.data = df
        st.session_state.db_cargada = True
        st.success(f"Base de datos cargada con {len(df)} registros usando Polars.")
        
    except sqlite3.OperationalError as e:
        st.error(f"Error al leer la tabla 'desvinculados'. El archivo podría estar corrupto o faltar la tabla: {e}")
        st.session_state.db_cargada = False
    except Exception as e:
        st.error(f"Error inesperado al cargar la DB: {e}")
        st.session_state.db_cargada = False
    finally:
        # 9. Limpieza crítica: Cerrar conexiones temporales y eliminar el archivo temporal
        if conn_disk:
             conn_disk.close()
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        # La conexión 'conn' queda abierta en memoria, pero Streamlit la gestionará.

def procesar_csv(uploaded_csv):
    """Procesa y normaliza el CSV, y lo fusiona con los datos existentes."""
    try:
        df_csv = pd.read_csv(uploaded_csv, encoding='utf-8')
        
        # 1. Renombrar columnas
        df_csv.rename(columns={k: v for k, v in CSV_TO_DB_MAPPING.items()}, inplace=True)
        
        # 2. Normalizar FECHA_ALTA
        df_csv['fecha_alta'] = df_csv['fecha_alta'].apply(normalizar_fecha)
        df_csv = df_csv[df_csv['fecha_alta'].notna()] # Elimina filas con fecha inválida
        
        # 3. Normalizar NORMALIZADO (Booleano 0/1)
        df_csv['normalizado'] = df_csv['normalizado'].apply(
            lambda x: 1 if str(x).lower().strip() in TRUE_VALUES else 0
        )
        
        # 4. Establecer valores por defecto para nuevos registros
        df_csv['estado'] = 'pendiente'
        df_csv['fecha_intervencion'] = datetime.now().strftime(DATE_FORMAT)
        
        # 5. Seleccionar solo las columnas necesarias y reordenar
        cols_to_keep = [col for col in DB_COLUMNS if col in df_csv.columns]
        df_csv = df_csv[cols_to_keep]

        # 6. Fusión (IMPORTANTE: Mantiene registros existentes y agrega nuevos)
        if 'data' in st.session_state:
            df_combined = pd.concat([st.session_state.data, df_csv]).drop_duplicates(subset=['nro_cli'], keep='first')
            st.session_state.data = df_combined
        else:
            st.session_state.data = df_csv

        st.success(f"CSV importado. Total de registros: {len(st.session_state.data)}")

    except Exception as e:
        st.error(f"Error durante el procesamiento del CSV: {e}")

def guardar_db(df):
    """Guarda el DataFrame modificado en un nuevo archivo DB para descarga."""
    conn = sqlite3.connect(':memory:')
    df.to_sql('desvinculados', conn, if_exists='replace', index=False)
    
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Crea un archivo DB temporal
        db_path = 'desvinculados_actualizado.db'
        conn_disk = sqlite3.connect(db_path)
        df.to_sql('desvinculados', conn_disk, if_exists='replace', index=False)
        conn_disk.close()
        
        # Añade el archivo DB al ZIP
        zipf.write(db_path, arcname=db_path)
        # Limpieza (necesario en entornos efímeros)
        os.remove(db_path)

    # El buffer ahora contiene el archivo zip.
    return buffer.getvalue()

# --- Interfaz de Usuario (Streamlit) ---

st.title("📋 Gestor Web de Desvinculados EPE")

# 1. Inicialización del estado de sesión
if 'db_cargada' not in st.session_state:
    st.session_state.db_cargada = False
if 'data' not in st.session_state:
    st.session_state.data = pd.DataFrame(columns=DB_COLUMNS)

# --- Controles de Carga ---
st.header("1. Carga de Datos")
col1, col2 = st.columns(2)

with col1:
    db_file = st.file_uploader("Cargar Base de Datos Existente (.db)", type=['db', 'sqlite'])
    if db_file and not st.session_state.db_cargada:
        cargar_db(db_file)

with col2:
    csv_file = st.file_uploader("Cargar Archivo CSV para Nuevos Registros", type=['csv'])
    if csv_file:
        procesar_csv(csv_file)
        
# --- Interfaz de Edición ---
if st.session_state.db_cargada or not st.session_state.data.empty:
    
    st.header(f"2. Edición de Registros ({len(st.session_state.data)} en memoria)")
    
    # 2.1 Preparación para la edición interactiva
    df_edit = st.session_state.data.copy()
    
    # Asegura que las fechas están como strings para ser editables
    df_edit['fecha_intervencion'] = df_edit['fecha_intervencion'].apply(
        lambda x: x if x else datetime.now().strftime(DATE_FORMAT)
    )

    # 2.2 Edición interactiva
    st.markdown("**Edite los campos 'estado' y 'fecha_intervencion' directamente en la tabla.**")
    
    # Crea una columna de selección para el estado que usa las opciones
    estado_config = st.column_config.SelectboxColumn(
        "Estado",
        help="Estado del registro de desvinculación",
        options=list(ESTADOS),
        required=True
    )
    # Crea una columna de fecha para la intervención
    fecha_config = st.column_config.DateColumn(
        "Fecha Intervención",
        help="Fecha de la última intervención (YYYY-MM-DD)",
        format=DATE_FORMAT,
        required=True
    )

    edited_df = st.data_editor(
        df_edit,
        column_config={
            "estado": estado_config,
            "fecha_intervencion": fecha_config
        },
        disabled=('nro_cli', 'nro_med', 'usuario', 'domicilio', 'normalizado', 'fecha_alta'),
        hide_index=True,
        key="data_editor"
    )

    # 3. Guardar cambios en el estado de sesión
    st.session_state.data = edited_df

    st.header("3. Finalizar y Exportar")
    
    # 3.1 Descarga de la Base de Datos
    st.download_button(
        label="💾 Descargar Base de Datos Actualizada (.db)",
        data=guardar_db(st.session_state.data),
        file_name='desvinculados_actualizado.db',
        mime='application/octet-stream',
        help="Guarda los cambios en la base de datos para persistencia."
    )

    # 3.2 Envío por Email (LÓGICA CRÍTICA PENDIENTE)
    st.markdown("""
    **🚨 Tarea Pendiente (Envío Email):** La funcionalidad de enviar el archivo adjunto a
    `lzurverra@epe.santafe.gov.ar` requiere un servicio SMTP externo y manejo de credenciales de seguridad (variables de entorno en Render).
    
    **Esto NO se puede implementar de manera simple y segura en este script de Streamlit.**
    """, unsafe_allow_html=True)
    
else:
    st.info("Por favor, cargue una Base de Datos existente o un CSV para comenzar a trabajar.")