
# def iniciar_servidor():
#     """
#     Inicia el servidor Uvicorn para la aplicación FastAPI.
#     """
#     uvicorn.run(
#         "api.app:app",  # Asegúrate de que esta ruta es correcta
#         host="0.0.0.0",
#         port=60801,
#         reload=True,
#         workers=2,
#         log_level="info",
#         access_log=True
#     )

# if __name__ == "__main__":
#     iniciar_servidor()

# from fastapi import FastAPI
# from apscheduler.schedulers.background import BackgroundScheduler
# import logging
# from contextlib import asynccontextmanager
# from pytz import timezone

# # Configuración del logging para guardar en archivo
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     handlers=[
#         logging.FileHandler("logs.log"),  # Guarda logs en logs.log
#         logging.StreamHandler()  # También imprime en consola
#     ]
# )


from fastapi import FastAPI, Query, HTTPException
import sys
sys.path.append('..')
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from functions.functions import *
import logging
from contextlib import asynccontextmanager
from pytz import timezone

# Configuración de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("API iniciada con tarea programada todos los días a las 10:00.")
    # Iniciar el scheduler dentro de la API
    scheduler = BackgroundScheduler()
    scheduler.add_job(ejecutar_clima, 'cron', hour=10, minute=1, timezone=timezone('America/Mexico_City'))
    scheduler.start()
    yield
    logging.info("Apagando el Scheduler.")
    scheduler.shutdown()
    logging.info("Scheduler apagado correctamente.")

# API y tarea en el mismo proceso
app = FastAPI(lifespan=lifespan)

# Endpoint manual para ejecutar la tarea si se desea
@app.get("/clima")
def obtener_clima(
    date_start: str = Query(None, description="Fecha de inicio en formato YYYY-MM-DD"),
    date_end: str = Query(None, description="Fecha de fin en formato YYYY-MM-DD"),
    api_key: str = Query(None, description="Tu API Key para Visual Crossing")
):
    # Usar la fecha actual si no se proporciona
    if not date_start:
        date_start = obtener_fecha_ayer()
    if not date_end:
        date_end = obtener_fecha_ayer()
    # Usar API Key por defecto si no se proporciona
    if not api_key:
        api_key = API_KEY
    try:
        start_date = datetime.strptime(date_start, "%Y-%m-%d")
        end_date = datetime.strptime(date_end, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de fecha inválido. Use YYYY-MM-DD.")
        # Validar que end sea mayor que start
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="ERROR: La fecha de fin debe ser mayor o igual que la de inicio.")
    
    # Llama a la tarea manualmente si el usuario accede a este endpoint
    ejecutar_clima(date_start,date_end, api_key)  
    return {"message": "Tarea ejecutada manualmente"}

