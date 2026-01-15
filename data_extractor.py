import httpx
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()
dsn = os.getenv("DATABASE_URL")


# --------------------------------------------------
# 🔹 Logging append-only (UN EVENTO = UNA FILA)
# --------------------------------------------------
async def log_report_event(conn, cliente, status, report_link=None):
    """
    Inserta un nuevo evento de log (append-only).
    Cada llamada genera una fila nueva.
    """
    await conn.execute(
        """
        INSERT INTO core.logs_generated_reports (
            cliente,
            status,
            report_link,
            date
        )
        VALUES ($1, $2, $3, NOW())
        """,
        cliente,
        status,
        report_link
    )


# --------------------------------------------------
# 🔹 Función principal: generación + descarga del reporte
# --------------------------------------------------
async def generate_report(api_key: str, cliente: str):
    """
    Genera y descarga un reporte desde la API de Reply.io.
    Cada evento del proceso queda registrado como una fila nueva en logs.
    """

    MAX_INTENTOS_PROCESO = 2   # Intentos globales (nuevo link)
    MAX_INTENTOS_DESCARGA = 3  # Intentos de descarga por link

    url_generar = "https://api.reply.io/api/v2/reports/generate-email-report"
    headers = {"x-api-key": api_key}
    params = {"reportType": "1"}

    print(f"\n🧾 Cliente: {cliente}")
    print(f"🚀 Iniciando generación de reporte para {cliente}...")

    conn = await asyncpg.connect(dsn=dsn)

    # 🟢 Inicio del proceso
    await log_report_event(
        conn,
        cliente,
        "PROCESS_START | global=- | download=- | inicio del proceso de generación"
    )

    # 🔁 Intentos globales
    for intento_proceso in range(1, MAX_INTENTOS_PROCESO + 1):

        print(f"\n🚀 Intento Global #{intento_proceso}")

        await log_report_event(
            conn,
            cliente,
            f"GENERATE_REQUEST | global={intento_proceso} | download=- | solicitando generación de reporte"
        )

        errores_descargas = []
        location = None

        # ---------------------------
        # Generación del reporte
        # ---------------------------
        try:
            async with httpx.AsyncClient() as client:
                resp_generar = await client.get(
                    url_generar,
                    headers=headers,
                    params=params
                )

                location = resp_generar.headers.get("location")
                if not location:
                    raise Exception("No se encontró header 'location'")

                print(f"➡️ URL de descarga: {location}")

                await log_report_event(
                    conn,
                    cliente,
                    f"GENERATE_SUCCESS | global={intento_proceso} | download=- | link generado correctamente",
                    report_link=location
                )

        except Exception as e:
            msg = f"GENERATE_ERROR | global={intento_proceso} | download=- | {str(e)}"
            print(f"❌ {msg}")

            await log_report_event(conn, cliente, msg)

            if intento_proceso < MAX_INTENTOS_PROCESO:
                delay = 60 * intento_proceso
                print(f"⏳ Esperando {delay} segundos antes de reintentar...")
                await asyncio.sleep(delay)
                continue
            else:
                await log_report_event(
                    conn,
                    cliente,
                    "PROCESS_FAILED | global=- | download=- | error generando link"
                )
                await conn.close()
                raise

        # ---------------------------
        # Intentos de descarga
        # ---------------------------
        for intento_descarga in range(1, MAX_INTENTOS_DESCARGA + 1):
            try:
                await log_report_event(
                    conn,
                    cliente,
                    f"DOWNLOAD_ATTEMPT | global={intento_proceso} | download={intento_descarga} | intentando descarga",
                    report_link=location
                )

                print(f"⬇️ Descarga #{intento_descarga} (Global {intento_proceso})")
                await asyncio.sleep(30)

                async with httpx.AsyncClient() as client:
                    resp_descarga = await client.get(location, headers=headers)

                if resp_descarga.status_code == 200:
                    print("✅ Descarga exitosa")

                    await log_report_event(
                        conn,
                        cliente,
                        f"DOWNLOAD_SUCCESS | global={intento_proceso} | download={intento_descarga} | descarga completada",
                        report_link=location
                    )

                    await log_report_event(
                        conn,
                        cliente,
                        f"PROCESS_SUCCESS | global={intento_proceso} | download={intento_descarga} | proceso completado"
                    )

                    await conn.close()

                    return {
                        "cliente": cliente,
                        "status_code": resp_descarga.status_code,
                        "headers": dict(resp_descarga.headers),
                        "body": resp_descarga.text
                    }

                else:
                    msg = (
                        f"DOWNLOAD_ERROR | global={intento_proceso} | "
                        f"download={intento_descarga} | status_code={resp_descarga.status_code}"
                    )
                    errores_descargas.append(msg)
                    print(f"⚠️ {msg}")

                    await log_report_event(conn, cliente, msg, report_link=location)

            except Exception as e:
                msg = (
                    f"DOWNLOAD_ERROR | global={intento_proceso} | "
                    f"download={intento_descarga} | exception={str(e)}"
                )
                errores_descargas.append(msg)
                print(f"❌ {msg}")

                await log_report_event(conn, cliente, msg, report_link=location)

            if intento_descarga < MAX_INTENTOS_DESCARGA:
                print("⏳ Esperando 60 segundos antes de reintentar descarga...")
                await asyncio.sleep(60)

        # ---------------------------
        # Se abandonan descargas de este link
        # ---------------------------
        await log_report_event(
            conn,
            cliente,
            f"DOWNLOAD_GIVE_UP | global={intento_proceso} | download=- | agotados intentos de descarga",
            report_link=location
        )

        if intento_proceso < MAX_INTENTOS_PROCESO:
            delay = 120 * intento_proceso
            print(f"🔄 Reintentando con nuevo link en {delay} segundos...")

            await log_report_event(
                conn,
                cliente,
                f"PROCESS_RETRY | global={intento_proceso + 1} | download=- | reintentando con nuevo link"
            )

            await asyncio.sleep(delay)

    # ---------------------------
    # Fallo total
    # ---------------------------
    await log_report_event(
        conn,
        cliente,
        "PROCESS_FAILED | global=- | download=- | no se pudo generar ni descargar el reporte"
    )

    await conn.close()
    raise Exception(
        f"No se pudo generar ni descargar el reporte para {cliente}"
    )
