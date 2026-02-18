from __future__ import annotations

import os
import time
import signal
import logging
from datetime import datetime, timezone, timedelta

import sqlite3
import mysql.connector
from mysql.connector import Error as MySQLError

from common import (
    setup_dirs,
    setup_logger,
    sqlite_connect,
    init_sqlite_queue,
    SQLITE_PATH,
    get_local_ip,
)

# -----------------------------
# Config (ENV)
# -----------------------------
DEVICE_KEY = os.getenv("DEVICE_KEY", "raspi-unknown")
DEVICE_LOCATION = os.getenv("DEVICE_LOCATION", "") or None
DEVICE_IP = os.getenv("DEVICE_IP", "") or None

SYNC_INTERVAL_SECONDS = float(os.getenv("SYNC_INTERVAL_SECONDS", "5"))
MYSQL_BATCH_SIZE = int(os.getenv("MYSQL_BATCH_SIZE", "200"))
MYSQL_CONNECT_TIMEOUT = int(os.getenv("MYSQL_CONNECT_TIMEOUT", "5"))
MYSQL_SSL_DISABLED = os.getenv("MYSQL_SSL_DISABLED", "1") == "1"

# Retenção local
ENABLE_RETENTION = os.getenv("ENABLE_RETENTION", "1") == "1"
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "90"))

# Dead-letter
MAX_SYNC_ATTEMPTS = int(os.getenv("MAX_SYNC_ATTEMPTS", "50"))

# MySQL
MYSQL_ENABLED = os.getenv("MYSQL_ENABLED", "1") == "1"
MYSQL_HOST = os.getenv("MYSQL_HOST", "")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "leathersense")
MYSQL_USER = os.getenv("MYSQL_USER", "")
MYSQL_PASS = os.getenv("MYSQL_PASS", "")


# -----------------------------
# SQLite ops
# -----------------------------
def fetch_unsynced(conn: sqlite3.Connection, limit: int) -> list[tuple]:
    cur = conn.execute(
        """
        SELECT reading_uuid, device_key, sensor_type, pin, ts_utc, temperature_c, humidity_pct, ok, error_msg, attempts
        FROM queue_readings
        WHERE synced=0 AND dead=0
        ORDER BY ts_epoch_utc ASC
        LIMIT ?
        """,
        (limit,),
    )
    return cur.fetchall()


def mark_synced(conn: sqlite3.Connection, uuids: list[str]) -> None:
    if not uuids:
        return
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn.executemany(
        """
        UPDATE queue_readings
        SET synced=1, last_attempt_utc=?, attempts=attempts+1
        WHERE reading_uuid=?
        """,
        [(now, u) for u in uuids],
    )
    conn.commit()


def mark_attempt_failed(conn: sqlite3.Connection, logger: logging.Logger, uuids: list[str], reason: str) -> None:
    if not uuids:
        return
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn.executemany(
        """
        UPDATE queue_readings
        SET last_attempt_utc=?, attempts=attempts+1
        WHERE reading_uuid=?
        """,
        [(now, u) for u in uuids],
    )
    conn.commit()
    logger.warning("Sync falhou (%s) para %s leituras", reason, len(uuids))


def mark_dead_if_exceeded(conn: sqlite3.Connection, logger: logging.Logger) -> None:
    cur = conn.execute(
        """
        UPDATE queue_readings
        SET dead=1
        WHERE synced=0 AND dead=0 AND attempts >= ?
        """,
        (MAX_SYNC_ATTEMPTS,),
    )
    conn.commit()
    if cur.rowcount:
        logger.warning("Dead-letter: %s registros marcados como dead (attempts >= %s)", cur.rowcount, MAX_SYNC_ATTEMPTS)


def retention_cleanup(conn: sqlite3.Connection, logger: logging.Logger) -> None:
    if not ENABLE_RETENTION:
        return
    cutoff_epoch = int((datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).timestamp())
    cur = conn.execute(
        """
        DELETE FROM queue_readings
        WHERE ts_epoch_utc < ?
          AND (synced=1 OR dead=1)
        """,
        (cutoff_epoch,),
    )
    conn.commit()
    if cur.rowcount:
        logger.info("Retention local: removidos %s registros (synced/dead) < epoch=%s", cur.rowcount, cutoff_epoch)


# -----------------------------
# MySQL ops (compatível com seu schema)
# devices(id, device_key, location, ip, created_at)
# sensors(id, device_id, sensor_type, pin, label, created_at)
# readings(... reading_uuid UNIQUE recomendado, sensor_id FK sensors.id, ts_utc, temperature_c, humidity_pct, ok, error_msg)
# -----------------------------
def mysql_connect():
    kwargs = dict(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DB,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        connection_timeout=MYSQL_CONNECT_TIMEOUT,
        autocommit=False,
        charset="utf8mb4",
        init_command="SET time_zone = '+00:00'",
    )
    if MYSQL_SSL_DISABLED:
        kwargs["ssl_disabled"] = True
    return mysql.connector.connect(**kwargs)


def ensure_device(cur, device_key: str, location: str | None, ip: str | None) -> int:
    cur.execute(
        """
        INSERT INTO devices (device_key, location, ip)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
          location = COALESCE(VALUES(location), location),
          ip       = COALESCE(VALUES(ip), ip)
        """,
        (device_key, location, ip),
    )
    cur.execute("SELECT id FROM devices WHERE device_key=%s", (device_key,))
    row = cur.fetchone()
    if not row:
        raise MySQLError("Falha ao obter devices.id após upsert.")
    return int(row[0])


def ensure_sensor(cur, device_id: int, sensor_type: str, pin: str, label: str) -> int:
    # tenta achar primeiro (evita duplicata se não tiver UNIQUE)
    cur.execute(
        "SELECT id FROM sensors WHERE device_id=%s AND sensor_type=%s AND pin=%s ORDER BY id DESC LIMIT 1",
        (device_id, sensor_type, pin),
    )
    row = cur.fetchone()
    if row:
        return int(row[0])

    # cria
    cur.execute(
        """
        INSERT INTO sensors (device_id, sensor_type, pin, label)
        VALUES (%s, %s, %s, %s)
        """,
        (device_id, sensor_type, pin, label),
    )
    return int(cur.lastrowid)


def push_batch_mysql(cur, batch: list[tuple]) -> None:
    """
    batch: (reading_uuid, sensor_id, ts_utc, temperature_c, humidity_pct, ok, error_msg)
    Recomendado ter UNIQUE(reading_uuid) na readings pra idempotência.
    """
    cur.executemany(
        """
        INSERT INTO readings (reading_uuid, sensor_id, ts_utc, temperature_c, humidity_pct, ok, error_msg)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          temperature_c = VALUES(temperature_c),
          humidity_pct  = VALUES(humidity_pct),
          ok            = VALUES(ok),
          error_msg     = VALUES(error_msg)
        """,
        batch,
    )


def is_transient_mysql_error(e: MySQLError) -> bool:
    # heurística simples (conexão, timeout, servidor caiu)
    transient_errnos = {1040, 1042, 1047, 1053, 1152, 1153, 1205, 1213, 2002, 2003, 2013}
    try:
        return getattr(e, "errno", None) in transient_errnos
    except Exception:
        return False


def push_with_split(cur, batch: list[tuple]) -> None:
    """
    Se 1 linha quebrar, divide o lote pra isolar poison pill.
    """
    if not batch:
        return
    try:
        push_batch_mysql(cur, batch)
        return
    except MySQLError:
        if len(batch) == 1:
            raise
        mid = len(batch) // 2
        push_with_split(cur, batch[:mid])
        push_with_split(cur, batch[mid:])


# -----------------------------
# Main loop
# -----------------------------
RUNNING = True


def handle_stop(signum, frame):
    global RUNNING
    RUNNING = False


def mysql_config_ok() -> tuple[bool, str]:
    missing = []
    if not MYSQL_HOST:
        missing.append("MYSQL_HOST")
    if not MYSQL_USER:
        missing.append("MYSQL_USER")
    if not MYSQL_PASS:
        missing.append("MYSQL_PASS")
    if not MYSQL_DB:
        missing.append("MYSQL_DB")
    ok = (len(missing) == 0)
    report = f"MYSQL_ENABLED={'1' if MYSQL_ENABLED else '0'} | HOST={MYSQL_HOST or '<vazio>'} | DB={MYSQL_DB or '<vazio>'} | USER={MYSQL_USER or '<vazio>'} | PASS={'<set>' if MYSQL_PASS else '<vazio>'} | MISSING={','.join(missing) if missing else '-'}"
    return ok, report


def main():
    setup_dirs()
    logger = setup_logger("leathersense_sync", "sync.log")

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    conn_sqlite = sqlite_connect()
    init_sqlite_queue(conn_sqlite)
    logger.info("SQLite OK: %s", SQLITE_PATH)

    cfg_ok, cfg_report = mysql_config_ok()
    if not MYSQL_ENABLED:
        logger.warning("MySQL desabilitado (MYSQL_ENABLED=0). Sync não rodará.")
    elif not cfg_ok:
        logger.warning("Sync OFF: config MySQL incompleta/ inválida | %s", cfg_report)

    mysql_ready = MYSQL_ENABLED and cfg_ok

    mysql_fail_count = 0
    next_try = 0.0

    last_maintenance_date = None

    logger.info(
        "Sync iniciado | device=%s | sqlite=%s | mysql=%s | intervalo=%ss",
        DEVICE_KEY,
        SQLITE_PATH,
        "ON" if mysql_ready else "OFF",
        SYNC_INTERVAL_SECONDS,
    )

    while RUNNING:
        # manutenção diária (retention + deadletter)
        today = datetime.now(timezone.utc).date()
        if last_maintenance_date != today:
            retention_cleanup(conn_sqlite, logger)
            mark_dead_if_exceeded(conn_sqlite, logger)
            last_maintenance_date = today

        if not mysql_ready:
            time.sleep(max(2, SYNC_INTERVAL_SECONDS))
            continue

        if time.time() < next_try:
            time.sleep(1.0)
            continue

        rows = fetch_unsynced(conn_sqlite, MYSQL_BATCH_SIZE)
        if not rows:
            time.sleep(SYNC_INTERVAL_SECONDS)
            continue

        uuids = [r[0] for r in rows]

        try:
            conn_mysql = mysql_connect()
            cur = conn_mysql.cursor()

            ip = DEVICE_IP or get_local_ip() or None
            device_id = ensure_device(cur, DEVICE_KEY, DEVICE_LOCATION, ip)

            # cache local (sensor_type,pin)->sensor_id
            sensor_cache: dict[tuple[str, str], int] = {}

            batch = []
            for (reading_uuid, device_key, sensor_type, pin, ts_utc, t, h, ok, err, attempts) in rows:
                key = (sensor_type, pin)
                if key not in sensor_cache:
                    label = f"{sensor_type}@{pin}"
                    sensor_cache[key] = ensure_sensor(cur, device_id, sensor_type, pin, label)

                sensor_id = sensor_cache[key]
                batch.append((reading_uuid, sensor_id, ts_utc, t, h, ok, err))

            # envia lote (com split se der ruim)
            push_with_split(cur, batch)
            conn_mysql.commit()
            cur.close()
            conn_mysql.close()

            mark_synced(conn_sqlite, uuids)

            mysql_fail_count = 0
            next_try = time.time()
            logger.info("Sync MySQL OK | enviados=%s", len(uuids))

        except MySQLError as e:
            mysql_fail_count += 1
            wait = min(300, 2 ** min(mysql_fail_count, 8))
            next_try = time.time() + wait

            # Se for erro "transiente", não é culpa de dado
            mark_attempt_failed(conn_sqlite, logger, uuids, f"MySQL: {e}")

            # Se for transiente, não vale “punir” demais; mas como já incrementa attempts,
            # o dead-letter só bate em 50+ tentativas.
            logger.warning("Retry em %ss (fail_count=%s)", wait, mysql_fail_count)

        except sqlite3.OperationalError as e:
            # ex: database is locked
            mysql_fail_count += 1
            wait = min(30, 2 ** min(mysql_fail_count, 5))
            next_try = time.time() + wait
            logger.warning("SQLite OperationalError: %s | retry em %ss", e, wait)

        except Exception as e:
            mysql_fail_count += 1
            wait = min(300, 2 ** min(mysql_fail_count, 8))
            next_try = time.time() + wait
            mark_attempt_failed(conn_sqlite, logger, uuids, f"Exception: {e}")
            logger.warning("Retry em %ss (fail_count=%s)", wait, mysql_fail_count)

    logger.info("Encerrando sync...")
    try:
        conn_sqlite.close()
    except Exception:
        pass
    logger.info("Finalizado.")


if __name__ == "__main__":
    main()
