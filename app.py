"""
Script — sincronización MySQL → Oracle

Sincroniza varias tablas configuradas extrayendo por lotes desde MySQL y cargando en Oracle.
Convierte tipos (DATE, TIMESTAMP, INTERVAL) y valida NUMBER(p,s) para evitar desbordes.
Inserta con MERGE “solo-insert” basado en ID (no duplica si ya existe; inserta si no).
Hace COMMIT por lote y solo crea ./out/ si hay errores, guardando CSV y LOG de rechazos.
Muestra resumen final: filas leídas, aceptadas y número/ubicación de rechazos.
"""

from __future__ import annotations
import os, sys, csv, pathlib
from datetime import datetime, date, time as dtime, timedelta
from decimal import Decimal, InvalidOperation
from typing import List, Tuple, Any, Dict, Iterable

import mysql.connector as mysql
import oracledb

# ─── Configuración MySQL ───────────────────────────────────────────
MYSQL = dict(
    host="172.18.8.34",
    port=3306,
    database="nemoq",
    user="nemoq",
    password="nemoq",
    connection_timeout=10,
    charset="utf8mb4",
    use_unicode=True,
    autocommit=True,
    use_pure=True,
)

# ─── Configuración Oracle ──────────────────────────────────────────
ORACLE_DSN  = "172.31.254.85:1521/SFEREPDB"
ORACLE_USER = "SF_AUX_SEVILLA"
ORACLE_PASS = "qS4H5iPlmg@Z"

# ─── Tuning ────────────────────────────────────────────────────────
BATCH_SIZE = 1000
COMMIT_EVERY = 1
OUT_DIR = pathlib.Path(__file__).resolve().parent / "out"   # ← se crea solo si hay errores

# ─── Tablas a sincronizar ──────────────────────────────────────────
TABLES_TO_SYNC = [
    {
        "mysql_table": "booked_history",
        "oracle_table": "BOOKED_HISTORY",
        "query": (
            "SELECT * FROM booked_history AS bh "
            "WHERE bh.idcenter = 4 "
            "  AND bh.datum >= CURDATE() - INTERVAL 1 DAY "
            "  AND bh.datum <  CURDATE() "
            "ORDER BY bh.datum, bh.idcenter"
            #"LIMIT 50"
        )
    },
    {
        "mysql_table": "saafe_operations",
        "oracle_table": "SAAFE_OPERATIONS",
        "query": (
            "SELECT * FROM saafe_operations AS so "
            "WHERE so.id_center = 4 "
            "  AND so.datum >= CURDATE() - INTERVAL 1 DAY "
            "  AND so.datum <  CURDATE() "
            "ORDER BY so.datum, so.id_center"
            #"LIMIT 50"
        )
    },
    {
        "mysql_table": "saafe_authorizations",
        "oracle_table": "SAAFE_AUTHORIZATIONS",
        "query": (
            "SELECT * FROM saafe_authorizations AS sa "
            "WHERE sa.id_center = 4 "
            "  AND sa.datum >= CURDATE() - INTERVAL 1 DAY "
            "  AND sa.datum <  CURDATE() "
            "ORDER BY sa.datum, sa.id_center"
            #"LIMIT 50"
        )
    },
    {
        "mysql_table": "medical_acts_history",
        "oracle_table": "MEDICAL_ACTS_HISTORY",
        "query": (
            "SELECT * FROM medical_acts_history AS mah "
            "WHERE mah.idcenter = 4 "
            "  AND mah.datum >= CURDATE() - INTERVAL 1 DAY "
            "  AND mah.datum <  CURDATE() "
            "ORDER BY mah.datum, mah.idcenter"
            #"LIMIT 50"
        )
    },
]

# ─── Columnas especiales ─────────────────
DATE_COLS       : List[str] = ["DATUM"]
TIMESTAMP_COLS  : List[str] = ["DATE_CREATED", "DATUM_STATUS", "DATE_RESEND_ALARM", "PAYED_DATE"]
INTERVAL_COLS   : List[str] = ["BOOKEDTIME", "MTA", "MTE"]
DATE_UP   = {c.upper() for c in DATE_COLS}
TS_UP     = {c.upper() for c in TIMESTAMP_COLS}
INTERV_UP = {c.upper() for c in INTERVAL_COLS}

# ─── NUMBER(p,s) (BKD_* y TODAY) ───────────────────────────────────
NUMERIC_SPECS: Dict[str, Tuple[int, int]] = {
    "ACK": (1, 0),
    "ACK_ANUL": (1, 0),
    "ACTIVITY_REGISTERED": (1, 0),
    "APPOINTMENT_TYPE": (1, 0),
    "ATTENDED": (1, 0),
    "BDELETED": (3, 0),
    "BVERIFIED": (1, 0),
    "ID": (10, 0),
    "IDBENEFIT": (6, 0),
    "IDCENTER": (3, 0),
    "ID_CYCLE": (10, 0),
    "IDSPECIALITY": (6, 0),
    "IDTAG": (3, 0),
    "IDUSER": (6, 0),
    "IOVERLAP": (3, 0),
    "ISPRINTABLE": (1, 0),
    "MOBILE_USER": (1, 0),
    "ORDER_CASER": (2, 0),
    "POINTS": (3, 0),
    "PRICE": (6, 2),
    "PRINTED": (1, 0),
    "PRIORITY": (2, 0),
    "REQUIRES_DU": (1, 0),
    "RETURN_TO_PARENT": (1, 0),
    "SAAFE_FACTURATION_COMPANY": (1, 0),
    "SAAFE_MANUAL_FACTURATION": (1, 0),
    "SAAFE_STATUS": (1, 0),
    "SEOGA_COMPANY": (1, 0),
    "STATUS": (3, 0),
}
# SAAFE_OPERATIONS
NUMERIC_SPECS.update({
    "ID_CENTER": (10, 0),
    "CANCELED": (1, 0),
    "ID_USER_OGS": (10, 0),
    "REPORT_UPLOAD_STATUS": (1, 0),
})
# SAAFE_AUTHORIZATIONS
NUMERIC_SPECS.update({
    "STATUS": (1, 0),
})
# MEDICAL_ACTS_HISTORY
NUMERIC_SPECS.update({
    "IDCENTER": (10, 0),
    "IDBOOKED": (10, 0),
    "TYPE": (1, 0),
    "ACK_ANUL": (1, 0),
    "SAAFE_MANUAL_FACTURATION": (1, 0),
    "PRICE": (10, 2),
})

# ─── Utils ─────────────────────────────────────────────────────────
def ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def parse_date_str(s: str) -> date | None:
    s = (s or "").strip()
    if not s: return None
    return datetime.strptime(s[:10], "%Y-%m-%d").date()

def parse_timestamp_str(s: str) -> datetime | None:
    s = (s or "").strip()
    if not s: return None
    return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")

def parse_interval_hms(s: str) -> timedelta | None:
    s = (s or "").strip()
    if not s: return None
    h, m, sec = s.split(":")
    if "." in sec:
        sec_i, micro = sec.split(".", 1)
        micro = (micro + "000000")[:6]
        return timedelta(hours=int(h), minutes=int(m), seconds=int(sec_i), microseconds=int(micro))
    return timedelta(hours=int(h), minutes=int(m), seconds=int(sec))

def number_fits_spec(val_str: str, p: int, s: int) -> bool:
    try:
        d = Decimal(val_str)
    except (InvalidOperation, ValueError):
        return False
    t = d.as_tuple()
    total_digits = len(t.digits)
    scale = -t.exponent if t.exponent < 0 else 0
    if scale > s: return False
    integer_digits = total_digits - scale
    if integer_digits < 0: integer_digits = 0
    return (integer_digits + scale) <= p

# ─── Extracción por lotes desde MySQL ──────────────────────────────
def mysql_batches(query: str, batch_size: int = 1000) -> Iterable[Tuple[List[str], List[Dict[str, Any]]]]:
    print("[MYSQL] Conectando…", flush=True)
    conn = mysql.connect(**MYSQL)
    print(f"[MYSQL] Conectado. Server: {getattr(conn, 'server_info', 'desconocido')}", flush=True)
    cur = conn.cursor(dictionary=True)
    print("[MYSQL] Ejecutando consulta…", flush=True)
    cur.execute(query)
    cols = list(cur.column_names)
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows: break
        yield cols, rows
    conn.close()
    print("[MYSQL] FIN", flush=True)

# ─── Conexión ORACLE ───────────────────────────────────────────────
def open_oracle():
    print("[ORACLE] Conectando…", flush=True)
    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=ORACLE_DSN)
    print("[ORACLE] Conectado.", flush=True)
    return conn

# ─── MERGE solo-insert por ID ──────────────────────────────────────
def build_merge_sql(table: str, colnames: List[str]) -> str:
    if "id" in colnames: id_col = "id"
    elif "ID" in colnames: id_col = "ID"
    else: raise ValueError("No se encontró columna ID en el resultado MySQL.")
    select_items = ", ".join(f":{i+1} {col}" for i, col in enumerate(colnames))
    on_clause = f"d.{id_col} = s.{id_col}"
    insert_cols = ", ".join(colnames)
    insert_vals = ", ".join(f"s.{c}" for c in colnames)
    sql = f"""
    MERGE INTO {table} d
    USING (SELECT {select_items} FROM dual) s
       ON ({on_clause})
     WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    return " ".join(sql.split())

# ─── Transformación fila ───────────────────────────────────────────
def convert_row(header: List[str], row: Dict[str, Any]) -> Tuple[Any, ...]:
    vals: List[Any] = []
    for col in header:
        val = row.get(col); col_u = col.upper()
        if val == "" or val is None:
            vals.append(None); continue
        if isinstance(val, datetime):
            if col_u in DATE_UP: vals.append(val.date())
            elif col_u in TS_UP: vals.append(val.replace(microsecond=0))
            else: vals.append(val)
            continue
        if isinstance(val, date) and not isinstance(val, datetime):
            if col_u in DATE_UP: vals.append(val)
            elif col_u in TS_UP: vals.append(datetime.combine(val, dtime.min))
            else: vals.append(val)
            continue
        if isinstance(val, dtime):
            hhmmss = val.isoformat()
            vals.append(parse_interval_hms(hhmmss) if col_u in INTERV_UP else hhmmss)
            continue
        if isinstance(val, timedelta):
            vals.append(val); continue
        if isinstance(val, str):
            if col_u in DATE_UP: vals.append(parse_date_str(val)); continue
            if col_u in TS_UP: vals.append(parse_timestamp_str(val)); continue
            if col_u in INTERV_UP: vals.append(parse_interval_hms(val)); continue
            spec = NUMERIC_SPECS.get(col_u)
            if spec:
                p, s = spec
                if not number_fits_spec(val, p, s):
                    raise ValueError(f"Numeric overflow for {col_u}='{val}' target NUMBER({p},{s})")
                vals.append(val); continue
            vals.append(val if val != "" else None); continue
        spec = NUMERIC_SPECS.get(col_u)
        if spec and isinstance(val, (int, float, Decimal)):
            p, s = spec
            if not number_fits_spec(str(val), p, s):
                raise ValueError(f"Numeric overflow for {col_u}='{val}' target NUMBER({p},{s})")
            vals.append(val); continue
        vals.append(val)
    return tuple(vals)

# ─── Carga por lotes con MERGE (devuelve errores, no escribe ficheros) ─────────
def upsert_batch(conn, table: str, header: List[str], batch_rows: List[Dict[str, Any]]) -> Tuple[int, List[Tuple[List[str], str]]]:
    merge_sql = build_merge_sql(table, header)
    cur = conn.cursor()
    converted: List[Tuple[Any, ...]] = []
    errors: List[Tuple[List[str], str]] = []  # (row_as_text, message)

    for r in batch_rows:
        try:
            converted.append(convert_row(header, r))
        except Exception as ex:
            row_txt = [str(r.get(h)) if r.get(h) is not None else "" for h in header]
            errors.append((row_txt, f"PRECHECK: {ex}"))

    if not converted:
        cur.close()
        return 0, errors

    cur.executemany(merge_sql, converted, batcherrors=True)
    batch_errs = cur.getbatcherrors()
    accepted = len(converted) - len(batch_errs)

    for e in batch_errs:
        off = e.offset
        src_tuple = converted[off]
        row_as_text = [str(v) if v is not None else "" for v in src_tuple]
        errors.append((row_as_text, e.message))

    cur.close()
    return accepted, errors

# ─── Main ──────────────────────────────────────────────────────────
def main():
    conn = open_oracle()
    try:
        for tdef in TABLES_TO_SYNC:
            mysql_table  = tdef["mysql_table"]
            oracle_table = tdef["oracle_table"]
            mysql_query  = tdef["query"]

            print(f"\n[JOB] {mysql_table} -> {oracle_table}")
            header: List[str] = []
            total_in = total_ok = 0
            lotes = 0
            reject_count = 0  # ← acumulador de rechazos

            # Lazy writers (solo si hay errores)
            rej_writer = None
            rej_log = None
            rejects_csv_path = None
            rejects_log_path = None

            def ensure_writers(hdr: List[str]):
                nonlocal rej_writer, rej_log, rejects_csv_path, rejects_log_path
                if rej_writer is None:
                    OUT_DIR.mkdir(exist_ok=True)
                    rejects_csv_path = OUT_DIR / f"rejects_{mysql_table}_{ts()}.csv"
                    rejects_log_path = OUT_DIR / f"rejects_{mysql_table}_{ts()}.log"
                    rej_writer = csv.writer(rejects_csv_path.open("w", encoding="utf-8-sig", newline=""), delimiter=";")
                    rej_writer.writerow(hdr + ["__REASON__"])
                    rej_log = rejects_log_path.open("w", encoding="utf-8")

            # Extrae e inserta por lotes
            for cols, rows in mysql_batches(mysql_query, BATCH_SIZE):
                if not header:
                    header = cols
                total_in += len(rows)
                accepted, errs = upsert_batch(conn, oracle_table, header, rows)
                total_ok += accepted

                # registrar rechazos (si los hay)
                if errs:
                    ensure_writers(header)
                    for row_txt, msg in errs:
                        rej_writer.writerow(row_txt + [msg])
                        rej_log.write(f"{msg} | VALUES={row_txt}\n")
                    reject_count += len(errs)  # ← sumamos

                lotes += 1
                if lotes % COMMIT_EVERY == 0:
                    conn.commit()
                    print(f"[ORACLE] Commit (lote {lotes}) | aceptadas acum: {total_ok}")

            conn.commit()

            # Cierra writers si existen
            if rej_log is not None:
                rej_log.close()

            # Resumen claro del job
            if reject_count > 0 and rejects_csv_path and rejects_log_path:
                print(f"[INFO] Rechazos: {reject_count} "
                      f"(CSV: {rejects_csv_path.name}, LOG: {rejects_log_path.name})")
            else:
                print("[INFO] Sin rechazos.")

            print(f"[DONE] {oracle_table}: leídas={total_in}, aceptadas≈{total_ok}")

    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print("[FATAL]", e, file=sys.stderr)
        sys.exit(1)
