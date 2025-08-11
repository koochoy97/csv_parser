import httpx
import asyncio

async def generate_report(api_key: str):
    MAX_INTENTOS_PROCESO = 2
    MAX_INTENTOS_DESCARGA = 3

    url_generar = "https://api.reply.io/api/v2/reports/generate-email-report"
    headers = {"x-api-key": api_key}
    params = {"reportType": "1"}

    for intento_proceso in range(1, MAX_INTENTOS_PROCESO + 1):
        print(f"\n🚀 Intento proceso completo #{intento_proceso}")

        # Generar el link de descarga
        try:
            async with httpx.AsyncClient() as client:
                print("🔹 Generando reporte...")
                resp_generar = await client.get(url_generar, headers=headers, params=params)
                print(f"📩 Status generación: {resp_generar.status_code}")
                print(f"📩 Headers generación: {resp_generar.headers}")

                location = resp_generar.headers.get("location")
                if not location:
                    raise Exception("❌ No se encontró header 'location' en la respuesta inicial")
                print(f"➡️ URL para descargar el reporte: {location}")
        except Exception as e:
            print(f"❌ Error generando link: {e}")
            if intento_proceso < MAX_INTENTOS_PROCESO:
                delay = 60 * intento_proceso  # aumento progresivo del delay
                print(f"⏳ Esperando {delay} segundos antes de reintentar proceso...")
                await asyncio.sleep(delay)
            else:
                raise

        # Intentar descargar el reporte hasta 3 veces
        for intento_descarga in range(1, MAX_INTENTOS_DESCARGA + 1):
            try:
                print(f"⬇️ Intento descarga #{intento_descarga} después de esperar 30 segundos...")
                await asyncio.sleep(30)  # espera antes de cada intento de descarga

                async with httpx.AsyncClient() as client:
                    resp_descarga = await client.get(location, headers=headers)
                    print(f"📩 Status descarga: {resp_descarga.status_code}")

                    # Verificar si el contenido parece correcto
                    if resp_descarga.status_code == 200:
                        print("✅ Reporte descargado correctamente.")
                        return {
                            "status_code": resp_descarga.status_code,
                            "headers": dict(resp_descarga.headers),
                            "body": resp_descarga.text
                        }
                    else:
                        print(f"⚠️ Respuesta inválida o reporte no listo aún (intento {intento_descarga}).")
            except Exception as e:
                print(f"❌ Error en la descarga: {e}")

            # Esperar 60 segundos entre intentos de descarga
            if intento_descarga < MAX_INTENTOS_DESCARGA:
                print("⏳ Esperando 60 segundos antes de reintentar descarga...")
                await asyncio.sleep(60)

        # Si no pudo descargar, espera más tiempo y vuelve a intentar todo el proceso
        if intento_proceso < MAX_INTENTOS_PROCESO:
            delay = 120 * intento_proceso  # delay más largo antes de reiniciar el proceso
            print(f"🔄 No se pudo descargar el reporte, esperando {delay} segundos antes de reiniciar proceso...")
            await asyncio.sleep(delay)

    raise Exception("❌ No se pudo generar ni descargar el reporte tras múltiples intentos.")


