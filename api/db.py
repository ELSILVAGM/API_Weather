from dotenv import load_dotenv
import pandas as pd
import os
import snowflake.connector
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy.exc import SQLAlchemyError
from snowflake.connector.errors import DatabaseError, ProgrammingError

# Cargar variables de entorno desde .env
def obtener_variable_env(nombre_variable):
    return os.getenv(nombre_variable)

# Conexión a Snowflake
SF_USER = obtener_variable_env("SF_USER")
SF_PASSWORD = obtener_variable_env("SF_PASSWORD")
SF_ACCOUNT = obtener_variable_env("SF_ACCOUNT")
SF_DATABASE = obtener_variable_env("SF_DATABASE")
SF_SCHEMA = obtener_variable_env("SF_SCHEMA")
SF_SCHEMA_CT = obtener_variable_env("SF_SCHEMA_CT")

# Conectarse a Snowflake con el conector oficial
def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PASSWORD,
            account=SF_ACCOUNT,
            database=SF_DATABASE,
            schema=SF_SCHEMA
        )
        print("Conexión a Snowflake exitosa.")
        return conn
    except DatabaseError as e:
        print(f"Error de conexión a Snowflake: {e}")
    except Exception as e:
        print(f"Error inesperado en Snowflake: {e}")
    return None  # Retorna None si la conexión falla

# Conectar usando SQLAlchemy
def get_sqlalchemy_conn():
    try:
        engine = create_engine(
            f'snowflake://{SF_USER}:{SF_PASSWORD}@{SF_ACCOUNT}/{SF_DATABASE}/{SF_SCHEMA_CT}'
        )
        conn = engine.connect()
        print("Conexión con SQLAlchemy exitosa.")
        return conn
    except SQLAlchemyError as e:
        print(f"Error en la conexión SQLAlchemy: {e}")
    except Exception as e:
        print(f"Error inesperado en SQLAlchemy: {e}")
    return None  # Retorna None si la conexión falla

# Insertar registros en snowflake
def insertar_sf(df):
    conn = get_snowflake_connection()
    conn_sql = get_sqlalchemy_conn()
    try:
        # Consultar solo los registros existentes en Snowflake
        query_existentes = f"SELECT TMP_ID, PAIS_ID, ESTADO_ID FROM DEV_STG.GNM.HIST_CLIMA"
        df_existente = pd.read_sql(query_existentes, conn_sql)
        df_existente.columns = df_existente.columns.str.upper()
        df_existente['TMP_ID']= df_existente['TMP_ID'].astype(str)
        merge_df = df.merge(df_existente, on=['TMP_ID', 'PAIS_ID', 'ESTADO_ID'], how='left', indicator=True)
        # Filtrar los registros nuevos de forma más eficiente
        df_a_insertar = merge_df[merge_df['_merge'] != 'both']
        if not df_a_insertar.empty:
            success, num_chunks, num_rows, output = write_pandas(conn,df_a_insertar,"HIST_CLIMA")  
            if success:
                print(f"Insertados {num_rows} filas en la tabla HIST_CLIMA")
            else:
                print(f"La operación no fue completamente exitosa. Salida: {output}")
        else:
            print('Datos existentes.')
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error en la ejecución SQL: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")
    finally:
        conn.close()



