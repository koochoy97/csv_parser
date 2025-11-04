import asyncio
import asyncpg
import csv
import io
from datetime import datetime
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de conexión a la base de datos desde .env
dsn = os.getenv("DATABASE_URL")

# Mapeo de columnas del CSV a columnas de la base de datos
CSV_COLUMNS = {
    'Contact Id': 'contact_id',
    'Contact First name': 'contact_first_name',
    'Contact Last name': 'contact_last_name',
    'Contact email': 'contact_email',
    'Contact country': 'contact_country',
    'Contact company': 'contact_company',
    'Contact industry': 'contact_industry',
    'Contact company size': 'contact_company_size',
    'Email account': 'email_account',
    'Sequence': 'sequence',
    'Sequence step': 'sequence_step',
    'Subject': 'subject',
    'Template': 'template',
    'Contacted': 'contacted',
    'Do not contact': 'do_not_contact',
    'Delivered': 'delivered',
    'Delivery date': 'delivery_date',
    'Opened': 'opened',
    'Opens': 'opens',
    'Replied': 'replied',
    'Interested': 'interested',
    'Not interested': 'not_interested',
    'Not now': 'not_now',
    'OptedOut': 'opted_out',
    'Bounced': 'bounced',
    'AutoReplied': 'auto_replied',
    'Forwarded': 'forwarded',
    'OutOfOffice': 'out_of_office',
    'Active': 'active',
    'Paused': 'paused',
    'Clicked': 'clicked',
    'Unsorted': 'unsorted',
}

COLUMN_MAPPING = {
    'contact_id': 'TEXT',
    'contact_first_name': 'TEXT',
    'contact_last_name': 'TEXT',
    'contact_email': 'TEXT',
    'contact_country': 'TEXT',
    'contact_company': 'TEXT',
    'contact_industry': 'TEXT',
    'contact_company_size': 'TEXT',
    'email_account': 'TEXT',
    'sequence': 'TEXT',
    'sequence_step': 'INTEGER',
    'subject': 'TEXT',
    'template': 'TEXT',
    'contacted': 'BOOLEAN',
    'do_not_contact': 'BOOLEAN',
    'delivered': 'BOOLEAN',
    'delivery_date': 'TIMESTAMP',
    'opened': 'BOOLEAN',
    'opens': 'INTEGER',
    'replied': 'BOOLEAN',
    'interested': 'BOOLEAN',
    'not_interested': 'BOOLEAN',
    'not_now': 'BOOLEAN',
    'opted_out': 'BOOLEAN',
    'bounced': 'BOOLEAN',
    'auto_replied': 'BOOLEAN',
    'forwarded': 'BOOLEAN',
    'out_of_office': 'BOOLEAN',
    'active': 'BOOLEAN',
    'paused': 'BOOLEAN',
    'clicked': 'BOOLEAN',
    'unsorted': 'BOOLEAN',
    'cliente': 'TEXT',
    'fecha_de_subida': 'TIMESTAMP',
    'sent_id': 'TEXT',
}


def convert(value, target_type):
    if value == '' or value is None:
        return None
    try:
        if target_type == 'TEXT':
            return str(value).strip()
        elif target_type == 'INTEGER':
            return int(value)
        elif target_type == 'BOOLEAN':
            return str(value).strip().lower() in ['true', '1', 'yes', 't', 'y']
        elif target_type == 'TIMESTAMP':
            for fmt in [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
                '%d/%m/%Y',
                '%m/%d/%Y',
                '%a, %d %b %Y %H:%M:%S %Z',
            ]:
                try:
                    return datetime.strptime(value.strip(), fmt)
                except ValueError:
                    continue
            return None
    except Exception:
        return None
    return value


def map_row(csv_row, cliente):
    result = {}
    for csv_col, db_col in CSV_COLUMNS.items():
        # Manejo especial para la primera columna que puede tener BOM
        actual_csv_col = csv_col
        if csv_col == 'Contact Id' and '\ufeffContact Id' in csv_row:
            actual_csv_col = '\ufeffContact Id'

        col_type = COLUMN_MAPPING[db_col]
        raw_value = csv_row.get(actual_csv_col, '')
        result[db_col] = convert(raw_value, col_type)

    result['cliente'] = cliente
    result['fecha_de_subida'] = datetime.now()

    contact_id = result.get('contact_id') or 'NA'
    email_account = result.get('email_account') or 'NA'
    sequence_step = result.get('sequence_step') or 'NA'
    delivery_date = result.get('delivery_date')

    if delivery_date and isinstance(delivery_date, datetime):
        delivery_date_str = delivery_date.strftime('%Y%m%d')
    else:
        delivery_date_str = 'nodate'

    result['sent_id'] = f"{contact_id}_{email_account}_{sequence_step}_{delivery_date_str}_{cliente}"
    return result


async def insert_bulk(conn, rows):
    if not rows:
        print("⚠️  No hay filas para insertar.")
        return

    columns = list(rows[0].keys())
    values = [tuple(row[col] for col in columns) for row in rows]

    query = f'''
        INSERT INTO staging.reporte_clientes ({','.join(columns)})
        VALUES ({','.join(f'${i + 1}' for i in range(len(columns)))})
    '''

    await conn.executemany(query, values)
    print(f'✅ {len(rows)} filas insertadas correctamente.')


async def main():
    print("🚀 Conectando a la base de datos...")
    conn = await asyncpg.connect(dsn=dsn)

    print("📥 Obteniendo datos crudos desde staging.reportes_clientes_raw...")
    raw_rows = await conn.fetch('SELECT id, raw_data, cliente_id FROM staging.reportes_clientes_raw')
    print(f'🗂️  {len(raw_rows)} archivos CSV encontrados.')

    total_inserted = 0

    for row in raw_rows:
        print(f"\n📄 Procesando archivo ID {row['id']} (cliente: {row['cliente_id']})...")

        log_id = None
        try:
            # Crear log de inicio
            log_id = await conn.fetchval(
                """
                INSERT INTO core.logs_csv_parser (cliente, status, processed_data, date)
                VALUES ($1, $2, NULL, NOW())
                RETURNING id
                """,
                row['cliente_id'],
                'Inicio de Parseo'
            )

            # Limpiar BOM si existe
            csv_data = row['raw_data']
            if csv_data.startswith('\ufeff'):
                csv_data = csv_data.lstrip('\ufeff')

            reader = csv.DictReader(io.StringIO(csv_data))
            parsed_rows = [map_row(csv_row, row['cliente_id']) for csv_row in reader]

            print(f"🧩 Parseadas {len(parsed_rows)} filas. Insertando en DB...")
            if parsed_rows:
                await insert_bulk(conn, parsed_rows)
                total_inserted += len(parsed_rows)

            # Actualizar log como exitoso
            await conn.execute(
                """
                UPDATE core.logs_csv_parser
                SET processed_data = $1,
                    status = $2,
                    date = NOW()
                WHERE id = $3
                """,
                len(parsed_rows),
                'Procesado',
                log_id
            )

        except Exception as e:
            print(f'❌ Error al procesar row_id={row["id"]}: {e}')

            # Actualizar log con error
            if log_id:
                await conn.execute(
                    """
                    UPDATE core.logs_csv_parser
                    SET status = $1,
                        date = NOW()
                    WHERE id = $2
                    """,
                    f"Error: {str(e)}"[:250],
                    log_id
                )
            else:
                # Si falla incluso antes de crear el log
                await conn.execute(
                    """
                    INSERT INTO core.logs_csv_parser (cliente, status, date)
                    VALUES ($1, $2, NOW())
                    """,
                    row['cliente_id'],
                    f"Error al iniciar: {str(e)}"[:250]
                )

    await conn.close()
    print("\n✅ Proceso finalizado. Conexión cerrada.")

    # Devuelve algo para el endpoint
    return {"total_files": len(raw_rows), "total_inserted_rows": total_inserted}


# Solo ejecuta si se llama directamente (útil para debugging)
if __name__ == "__main__":
    asyncio.run(main())

