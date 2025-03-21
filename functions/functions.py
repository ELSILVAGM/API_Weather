# Importar librerías
import pandas as pd
import requests
import json
from pathlib import Path
import sqlite3
import ijson
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from db import get_sqlalchemy_conn, obtener_variable_env, insertar_sf
import logging
# Configurar logging para que GitHub Actions lo capture
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# Configuración base para la API de clima
URL_BASE = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
API_KEY = obtener_variable_env("API_KEY")

# Obtener coordenadas de regiones desde Snowflake
def obtener_coordenadas():
    conn = get_sqlalchemy_conn()
    query = "SELECT * FROM  PRD_STG.GNM_CT.CAT_REGIONES_COORDENADAS; -- WHERE PAISID IN (1, 15,12, 13) AND IDESTADO IN (1,2,3);"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Función para obtener la fecha actual en formato YYYY-MM-DD
def obtener_fecha_ayer():
    # Obtener la fecha de hoy
    fecha_hoy = datetime.today()
    # Restar un día
    fecha_ayer = fecha_hoy - timedelta(days=1)
    # Formatear la fecha como 'YYYY-MM-DD'
    return fecha_ayer.strftime('%Y-%m-%d')

# Función para generar fechas dentro del rango
def generar_fechas(inicio, fin):
    try:
        inicio = datetime.strptime(inicio, '%Y-%m-%d')
        fin = datetime.strptime(fin, '%Y-%m-%d')
        return [(inicio + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((fin - inicio).days + 1)]
    except Exception as e:
        print(f"Error al generar fechas: {e}")
        return []


def guardar_json(nuevos_datos, nombre_archivo="datos.jsonl"):
    try:
        # Añadir los nuevos datos al archivo
        with open(nombre_archivo, mode="a", encoding="utf-8") as archivo:
            for dato in nuevos_datos:
                archivo.write(json.dumps(dato) + "\n")
        print(f"Datos añadidos y guardados en {nombre_archivo}")
    except Exception as e:
        print(f"Error inesperado: {e}")

# Obtener datos climáticos para una fila del DataFrame
def solicitud_APIclima(row, date_start, date_end, api_key):
    try:
        coordenadas = f"{row['latitud']},{row['longitud']}"
        url = f"{URL_BASE}{coordenadas}/{date_start}/{date_end}?key={api_key}&include=days&unitGroup=metric"
        response = requests.get(url)
        response.raise_for_status()
        weather_data = response.json()
        dias = weather_data.get('days', [])
        guardar_json(dias, nombre_archivo=r"C:\Users\elsilva\Documents\FEB\API_Weather\.json\weather_data.json")
        fechas_esperadas = generar_fechas(date_start, date_end)
        dias_procesados = []

        for fecha_esperada in fechas_esperadas:
            dia_encontrado = next((dia for dia in dias if dia['datetime'] == fecha_esperada), None)
            if dia_encontrado:
                dia_encontrado['paisid'] = row['paisid']
                dia_encontrado['idestado'] = row['idestado']
                dia_encontrado['TmpID'] = fecha_esperada.replace('-', '')
                dias_procesados.append(dia_encontrado)
            else:
                print(f"Fecha {fecha_esperada} no encontrada para {row['pais']}, {row['idestado']}")
        return dias_procesados
    except Exception as e:
        print(f"Error inesperado al procesar {row['pais']}, {row['idestado']}: {e}")
        return None

# Ejecutar solicitudes en paralelo
def procesar_filas_paralelamente(df, date_start, date_end, api_key):

    resultados = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(solicitud_APIclima, row, date_start, date_end, api_key): row for _, row in df.iterrows()}
        for future in as_completed(futures):
            try:
                resultado = future.result()
                if resultado:
                    resultados.extend(resultado)
            except Exception as e:
                print(f"Error en una tarea: {e}")
    return resultados

def homologar_columnas(df):
    data_final = df.copy()
    # Diccionario de mapeo de nombres de columnas
    column_mapping = {
        "TmpID": "TMP_ID",
        "paisid": "PAIS_ID",
        "idestado": "ESTADO_ID",
        "tempmax": "TEMP_MAX",
        "tempmin": "TEMP_MIN",
        "temp": "TEMP_PROMEDIO",
        "feelslikemax": "SENS_TERM_MAX",
        "feelslikemin": "SENS_TERM_MIN",
        "feelslike": "SENS_TERM_PROMEDIO",
        "dew": "PUNTO_ROCIO",
        "humidity": "HUMEDAD",
        "precip": "PRECIPITACION",
        "precipprob": "PROB_PRECIPITACION",
        "precipcover": "COBERTURA_PRECIPITACION",
        "preciptype": "TIPO_PRECIPITACION",
        "snow": "NIEVE",
        "snowdepth": "PROFUNDIDAD_NIEVE",
        "windgust": "RAFAGA_VIENTO",
        "windspeed": "VELOCIDAD_VIENTO",
        "winddir": "DIRECCION_VIENTO",
        "pressure": "PRESION",
        "sealevelpressure": "PRESION",
        "cloudcover": "COBERTURA_NUBOSA",
        "visibility": "VISIBILIDAD",
        "solarradiation": "RADIACION_SOLAR",
        "solarenergy": "ENERGIA_SOLAR",
        "uvindex": "INDICE_UV",
        "sunrise": "AMANECER",
        "sunriseEpoch": "AMANECER_EPOCH",
        "sunset": "OCASO",
        "sunsetEpoch": "OCASO_EPOCH",
        "moonphase": "FASE_LUNAR",
        "conditions": "CONDICIONES",
        "description": "DESCRIPCION",
        "icon": "ICONO",
        "stations": "ESTACIONES",
        "source": "FUENTE",
        "severerisk":"RIESGO_SEVERO"
    }
   
    # Filtrar solo las columnas que existen en el DataFrame
    columnas_existentes = [col for col in column_mapping.keys() if col in data_final.columns]
    # Aplicar el filtrado y renombrado solo con las columnas existentes
    data_final = data_final[columnas_existentes]
    data_final.rename(columns=column_mapping, inplace=True)
    return data_final

def ejecutar_clima(date_start=None,date_end=None,api_key=None):
    try:
        logging.info(f"Iniciando ejecución de ejecutar_clima con rango {date_start} - {date_end}")
        df_cordenadas = obtener_coordenadas()
        logging.info(f"Se obtuvieron {len(df_cordenadas)} coordenadas de regiones.")    
        datos_clima = procesar_filas_paralelamente(df_cordenadas, date_start, date_end, api_key)
        if not datos_clima:
            logging.warning("No se encontraron datos climáticos.")
            return {"message": "No se encontraron datos climáticos."}
        guardar_json(datos_clima, nombre_archivo=r"C:\Users\elsilva\Documents\FEB\API_Weather\.json\datos_clima.json")
        df_clima = pd.DataFrame(datos_clima)
        data_final = homologar_columnas(df_clima)
        logging.info(f"Datos climáticos transformados correctamente.")
        insertar_sf(data_final)
        logging.info("Datos climáticos insertados exitosamente en Snowflake.")
    except Exception as e:
        logging.error(f"Error al obtener datos climáticos: {e}", exc_info=True)