from fastapi import FastAPI
import asyncio
from worker import main as worker_main
from data_extractor import generate_report as data_extractor_main

app = FastAPI()

@app.get("/")
def root():
    return {"message": "API para procesamiento de CSV"}

@app.post("/procces_data")
async def run_job():
    # Aquí no creamos tarea en background, sino que esperamos a que termine
    result = await worker_main()
    return {"status": "Proceso finalizado", "detalle": result}


@app.post("/extract_reply_mails_reports")
async def extract_reply_mails_reports(api_key: str):
    result = await data_extractor_main(api_key)
    return result
