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
    asyncio.create_task(worker_main())  # ejecuta en segundo plano
    return {"status": "Proceso iniciado"}


@app.post("/extract_reply_mails_reports")
async def extract_reply_mails_reports(api_key: str):
    result = await data_extractor_main(api_key)
    return result
