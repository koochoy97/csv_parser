import httpx
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()
dsn = os.getenv("DATABASE_URL")


# --------------------------------------------------
# 🔹 Función de logging en DB
# --------------------------------------------------
async def log_report_event(conn, cliente, status, report_link=None):
    """
    Inserta o actualiza un log en core.logs_generated_reports.
    Mantiene una sola fila por cliente/report_link, actualizando status y fecha.
    """
    if report_link:
        existing = await conn.fetchval(
            """
            SELECT id FROM core.logs_generated_reports
            WHERE report_link = $1
            ORDER BY date DESC
            LIMIT 1
            """,
            report_link
        )

        if existing:
            await conn.execute(
                """
                UPDATE core.logs_generated_reports
                SET status = $1, date = NOW()
                WHERE id = $2
                """,
                status,
                existing
            )
            return
        else:
            await conn.execute(
                """
                INSERT INTO core.logs_generated_reports (cliente, status, report_link, date)
                VALUES ($1, $2, $3, NOW())
                """,
                cliente,
                status,
                report_link
            )
    else:
        existing_cliente = await conn.fetchval(
            """
            SELECT id FROM core.logs_generated_reports
            WHERE cliente = $1
            ORDER BY date DESC
            LIMIT 1
            """,
            cliente
        )

        if existing_cliente:
            await conn.execute(
                """
                UPDATE core.logs_generated_reports
                SET status = $1, date = NOW()
                WHERE id = $2
                """,
                status,
                existing_cliente
            )
        else:
            await conn.execute(
                """
                INSERT INTO core.logs_generated_reports (cliente, status, report_link, date)
                VALUES ($1, $2, NULL, NOW())
                """,
                cliente,
                status
            )


# --------------------------------------------------
# 🔹 Función principal: generación + descarga del reporte
# --------------------------------------------------
async def generate_report(api_key: str, cliente: str):
    """
    Genera y descarga un reporte desde la API de Reply.io,
    registrando cada cambio de estado en core.logs_generated_reports.
    """
    MAX_INTENTOS_PROCESO = 2   # Intentos globales de generar un nuevo link
    MAX_INTENTOS_DESCARGA = 3  # Intentos internos de descarga para cada link

    url_generar = "https://api.reply.io/api/v2/reports/generate-email-report"
    headers = {"x-api-key": api_key}
    params = {"reportType": "1"}

    print(f"\n🧾 Cliente: {cliente}")
    print(f"🚀 Iniciando generación de reporte para {cliente}...")

    conn = await asyncpg.connect(dsn=dsn)
    await log_report_event(conn, cliente, "Reporte Solicitado")

    # 🔁 Intentos globales (cada uno genera un nuevo link)
    for intento_proceso in range(1, MAX_INTENTOS_PROCESO + 1):
        print(f"\n🚀 Intento Global #{intento_proceso}")

        errores_descargas = []  # Acumula mensajes de error de descarga de este intento
        location = None

        try:
            async with httpx.AsyncClient() as client:
                print("🔹 Solicitando generación del reporte...")
                resp_generar = await client.get(url_generar, headers=headers, params=params)
                print(f"📩 Status generación: {resp_generar.status_code}")

                location = resp_generar.headers.get("location")
                if not location:
                    raise Exception("No se encontró header 'location' en la respuesta inicial")

                print(f"➡️ URL para descargar el reporte: {location}")
                await log_report_event(conn, cliente, f"Link Generado (Intento Global {intento_proceso})", report_link=location)

        except Exception as e:
            msg = f"Error generando link (Intento Global {intento_proceso}): {str(e)}"
            print(f"❌ {msg}")
            await log_report_event(conn, cliente, msg)
            if intento_proceso < MAX_INTENTOS_PROCESO:
                delay = 60 * intento_proceso
                print(f"⏳ Esperando {delay} segundos antes de reintentar...")
                await asyncio.sleep(delay)
                continue
            else:
                await conn.close()
                raise

        # 🔁 Intentos de descarga para este link
        for intento_descarga in range(1, MAX_INTENTOS_DESCARGA + 1):
            try:
                print(f"⬇️ Intento de descarga #{intento_descarga} (Intento Global {intento_proceso})...")
                await asyncio.sleep(30)

                async with httpx.AsyncClient() as client:
                    resp_descarga = await client.get(location, headers=headers)
                    print(f"📩 Status descarga: {resp_descarga.status_code}")

                    if resp_descarga.status_code == 200:
                        print(f"✅ Reporte descargado correctamente para {cliente}.")
                        await log_report_event(conn, cliente, f"Reporte Descargado (Intento Global {intento_proceso})", report_link=location)
                        await conn.close()
                        return {
                            "cliente": cliente,
                            "status_code": resp_descarga.status_code,
                            "headers": dict(resp_descarga.headers),
                            "body": resp_descarga.text
                        }
                    else:
                        msg = f"Descarga fallida (intento {intento_descarga}) - status {resp_descarga.status_code}"
                        errores_descargas.append(msg)
                        print(f"⚠️ {msg}")
                        await log_report_event(conn, cliente, msg, report_link=location)

            except Exception as e:
                msg = f"Error en descarga (intento {intento_descarga}): {str(e)}"
                errores_descargas.append(msg)
                print(f"❌ {msg}")
                await log_report_event(conn, cliente, msg, report_link=location)

            if intento_descarga < MAX_INTENTOS_DESCARGA:
                print("⏳ Esperando 60 segundos antes de reintentar descarga...")
                await asyncio.sleep(60)

        # ❌ Si se terminaron los intentos de descarga para este link
        msg_final = f"Error Final (Intento Global {intento_proceso}): {'; '.join(errores_descargas)}"
        print(f"❌ {msg_final}")
        await log_report_event(conn, cliente, msg_final, report_link=location)

        # ⏳ Esperar antes de generar un nuevo link
        if intento_proceso < MAX_INTENTOS_PROCESO:
            delay = 120 * intento_proceso
            print(f"🔄 Reintentando con un nuevo link después de {delay} segundos...")
            await asyncio.sleep(delay)

    # 🚨 Si llegamos acá, todos los intentos globales fallaron
    await log_report_event(conn, cliente, "Error Final - No se pudo generar ni descargar ningún reporte")
    await conn.close()
    raise Exception(f"❌ No se pudo generar ni descargar el reporte para {cliente} tras múltiples intentos globales.")



