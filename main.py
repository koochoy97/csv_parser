from fastapi import FastAPI
from worker import main as worker_main
from data_extractor import generate_report as data_extractor_main
from manual_csv_parser import manual_csv_parser

app = FastAPI()

@app.get("/")
def root():
    return {"message": "API para procesamiento de CSV"}

# =========================
# Worker original (no cambia)
# =========================
@app.post("/process_data")
async def run_job():
    result = await worker_main()
    return {"status": "Proceso finalizado", "detalle": result}


# =========================
# Nuevo servicio MANUAL
# =========================
@app.post("/manual_csv_parser")
async def run_manual_csv_parser(cliente_id: str):
    result = await manual_csv_parser(cliente_id)
    return {
        "status": "Proceso manual finalizado",
        "detalle": result
    }


@app.post("/extract_reply_mails_reports")
async def extract_reply_mails_reports(api_key: str, cliente: str):
    result = await data_extractor_main(api_key, cliente)
    return result
