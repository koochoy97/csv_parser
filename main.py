from fastapi import FastAPI
import asyncio
from worker import main as worker_main

app = FastAPI()

@app.get("/")
def root():
    return {"message": "API para procesamiento de CSV"}

@app.post("/procces_data")
async def run_job():
    asyncio.create_task(worker_main())  # ejecuta en segundo plano
    return {"status": "Proceso iniciado"}
