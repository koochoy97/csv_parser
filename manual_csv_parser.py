import asyncpg
import csv
import io
from datetime import datetime
import os
from dotenv import load_dotenv

# ======================
# Configuración
# ======================
load_dotenv()
dsn = os.getenv("DATABASE_URL")

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


# ======================
# Helpers
# ======================
def convert(value, target_type):
    if value in ('', None):
        return None
    try:
        if target_type == 'TEXT':
            return str(value).strip()
        if target_type == 'INTEGER':
            return int(value)
        if target_type == 'BOOLEAN':
            return str(value).strip().lower() in ['true', '1', 'yes', 'y', 't']
        if target_type == 'TIMESTAMP':
            for fmt in (
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
                '%d/%m/%Y',
                '%m/%d/%Y',
                '%a, %d %b %Y %H:%M:%S %Z',
                '%a, %d %b %Y %H:%M:%S'
            ):
                try:
                    return datetime.strptime(value.strip(), fmt)
                except ValueError:
                    continue
    except Exception:
        return None
    return None


def map_row(csv_row, cliente_id):
    result = {}

    for csv_col, db_col in CSV_COLUMNS.items():
        actual_col = csv_col
        if csv_col == 'Contact Id' and '\ufeffContact Id' in csv_row:
            actual_col = '\ufeffContact Id'

        raw_value = csv_row.get(actual_col, '')
        result[db_col] = convert(raw_value, COLUMN_MAPPING[db_col])

    result['cliente'] = cliente_id
    result['fecha_de_subida'] = datetime.now()

    contact_id = result.get('contact_id') or 'NA'
    email_account = result.get('email_account') or 'NA'
    sequence_step = result.get('sequence_step') or 'NA'
    delivery_date = result.get('delivery_date')

    date_str = delivery_date.strftime('%Y%m%d') if delivery_date else 'nodate'
    result['sent_id'] = f"{contact_id}_{email_account}_{sequence_step}_{date_str}_{cliente_id}"

    return result


async def insert_bulk(conn, rows):
    if not rows:
        return 0

    columns = list(rows[0].keys())
    values = [tuple(r[col] for col in columns) for r in rows]

    query = f"""
        INSERT INTO staging.reporte_clientes ({','.join(columns)})
        VALUES ({','.join(f'${i+1}' for i in range(len(columns)))})
    """

    await conn.executemany(query, values)
    return len(rows)


# ======================
# MAIN SERVICE
# ======================
async def manual_csv_parser(cliente_id: str):
    conn = await asyncpg.connect(dsn=dsn)

    raw_rows = await conn.fetch(
        """
        SELECT id, raw_data
        FROM staging.reportes_clientes_raw
        WHERE cliente_id = $1
        """,
        cliente_id
    )

    total_inserted = 0
    total_files = len(raw_rows)

    for row in raw_rows:
        log_id = None
        try:
            log_id = await conn.fetchval(
                """
                INSERT INTO core.logs_csv_parser (cliente, status, processed_data, date)
                VALUES ($1, 'Inicio manual', NULL, NOW())
                RETURNING id
                """,
                cliente_id
            )

            csv_data = row['raw_data']
            if csv_data.startswith('\ufeff'):
                csv_data = csv_data.lstrip('\ufeff')

            reader = csv.DictReader(io.StringIO(csv_data))
            parsed_rows = [map_row(r, cliente_id) for r in reader]

            inserted = await insert_bulk(conn, parsed_rows)
            total_inserted += inserted

            await conn.execute(
                """
                UPDATE core.logs_csv_parser
                SET status = 'Procesado manual',
                    processed_data = $1,
                    date = NOW()
                WHERE id = $2
                """,
                inserted,
                log_id
            )

        except Exception as e:
            if log_id:
                await conn.execute(
                    """
                    UPDATE core.logs_csv_parser
                    SET status = $1,
                        date = NOW()
                    WHERE id = $2
                    """,
                    f"Error manual: {str(e)}"[:250],
                    log_id
                )

    await conn.close()

    return {
        "cliente_id": cliente_id,
        "total_files": total_files,
        "total_inserted_rows": total_inserted
    }
