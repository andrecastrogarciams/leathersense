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
# Configurações (ENV)
# -----------------------------
DEVICE_KEY = os.getenv("DEVICE_KEY", "raspi-unknown")
DEVICE_LOCATION = os.getenv("DEVICE_LOCATION", "") or None
DEVICE_IP = os.getenv("DEVICE_IP", "") or None

SYNC_INTERVAL_SECONDS = float(os.getenv("SYNC_INTERVAL_SECONDS", "5"))
MYSQL_BATCH_SIZE = int(os.getenv("MYSQL_BATCH_SIZE", "200")) # Quantos registros enviar por vez
MYSQL_CONNECT_TIMEOUT = int(os.getenv("MYSQL_CONNECT_TIMEOUT", "5"))
MYSQL_SSL_DISABLED = os.getenv("MYSQL_SSL_DISABLED", "1") == "1"

# Retenção: Evita que o cartão SD do Raspberry Pi lote
ENABLE_RETENTION = os.getenv("ENABLE_RETENTION", "1") == "1"
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "90")) # Apaga dados locais após 90 dias

# Dead-letter: Se um registro falhar 50 vezes (ex: erro de formato), ele é ignorado para não travar a fila
MAX_SYNC_ATTEMPTS = int(os.getenv("MAX_SYNC_ATTEMPTS", "50"))

# Credenciais MySQL (Nuvem/Central)
MYSQL_ENABLED = os.getenv("MYSQL_ENABLED", "1") == "1"
MYSQL_HOST = os.getenv("MYSQL_HOST", "")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "leathersense")
MYSQL_USER = os.getenv("MYSQL_USER", "")
MYSQL_PASS = os.getenv("MYSQL_PASS", "")


# -----------------------------
# Operações SQLite (Local)
# -----------------------------
def fetch_unsynced(conn: sqlite3.Connection, limit: int) -> list[tuple]:
    """Busca registros que ainda não foram sincronizados e não estão 'mortos'."""
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
    """Marca com sucesso os registros enviados para a nuvem."""
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
    """Incrementa o contador de tentativas quando o envio falha."""
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
    """Move registros problemáticos para o estado 'dead' para evitar loops infinitos de erro."""
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
    """Limpeza periódica: remove dados velhos que já foram sincronizados ou descartados."""
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
        logger.info("Retention local: removidos %s registros < epoch=%s", cur.rowcount, cutoff_epoch)


# -----------------------------
# Operações MySQL (Remoto)
# -----------------------------
def mysql_connect():
    """Cria conexão com o servidor MySQL central."""
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
    """Garante que o Raspberry Pi está cadastrado na tabela de devices da nuvem (Upsert)."""
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
    """Garante que o sensor (DHT11/22) está cadastrado na nuvem vinculado a este dispositivo."""
    cur.execute(
        "SELECT id FROM sensors WHERE device_id=%s AND sensor_type=%s AND pin=%s ORDER BY id DESC LIMIT 1",
        (device_id, sensor_type, pin),
    )
    row = cur.fetchone()
    if row:
        return int(row[0])

    cur.execute(
        "INSERT INTO sensors (device_id, sensor_type, pin, label) VALUES (%s, %s, %s, %s)",
        (device_id, sensor_type, pin, label),
    )
    return int(cur.lastrowid)


def push_with_split(cur, batch: list[tuple]) -> None:
    """
    Técnica de Divisão de Lote: Se o MySQL rejeitar o lote (ex: um registro corrompido),
    ele divide o lote ao meio e tenta novamente. Isso isola o registro problemático (poison pill).
    """
    if not batch:
        return
    try:
        cur.executemany(
            """
            INSERT INTO readings (reading_uuid, sensor_id, ts_utc, temperature_c, humidity_pct, ok, error_msg)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              temperature_c = VALUES(temperature_c), ok = VALUES(ok)
            """,
            batch,
        )
    except MySQLError:
        if len(batch) == 1:
            raise # Se sobrou só um e deu erro, ele será tratado pelo contador de attempts
        mid = len(batch) // 2
        push_with_split(cur, batch[:mid])
        push_with_split(cur, batch[mid:])


# -----------------------------
# Loop Principal
# -----------------------------
RUNNING = True

def handle_stop(signum, frame):
    global RUNNING
    RUNNING = False

def main():
    setup_dirs()
    logger = setup_logger("leathersense_sync", "sync.log")
    
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    conn_sqlite = sqlite_connect()
    
    # Verifica se as configurações do MySQL estão presentes
    mysql_ready = MYSQL_ENABLED and MYSQL_HOST and MYSQL_USER

    mysql_fail_count = 0
    next_try = 0.0
    last_maintenance_date = None

    while RUNNING:
        # Rotina diária de manutenção do banco local
        today = datetime.now(timezone.utc).date()
        if last_maintenance_date != today:
            retention_cleanup(conn_sqlite, logger)
            mark_dead_if_exceeded(conn_sqlite, logger)
            last_maintenance_date = today

        if not mysql_ready or time.time() < next_try:
            time.sleep(1.0)
            continue

        # Pega um lote de registros pendentes
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

            # Prepara o lote vinculando cada leitura ao ID do sensor no MySQL
            sensor_cache = {}
            batch = []
            for (r_uuid, d_key, s_type, pin, ts, t, h, ok, err, att) in rows:
                key = (s_type, pin)
                if key not in sensor_cache:
                    sensor_cache[key] = ensure_sensor(cur, device_id, s_type, pin, f"{s_type}@{pin}")
                
                batch.append((r_uuid, sensor_cache[key], ts, t, h, ok, err))

            # Envia para a nuvem
            push_with_split(cur, batch)
            conn_mysql.commit()
            
            # Se chegou aqui, deu certo: atualiza SQLite
            mark_synced(conn_sqlite, uuids)
            mysql_fail_count = 0
            logger.info("Sync MySQL OK | enviados=%s", len(uuids))

        except (MySQLError, sqlite3.OperationalError) as e:
            mysql_fail_count += 1
            # Backoff Exponencial: espera mais tempo a cada erro seguido (até 5 min)
            wait = min(300, 2 ** min(mysql_fail_count, 8))
            next_try = time.time() + wait
            mark_attempt_failed(conn_sqlite, logger, uuids, str(e))
            logger.warning("Erro no Sync. Próxima tentativa em %ss", wait)
        
        finally:
            # Garante fechamento das conexões em cada ciclo
            if 'cur' in locals(): cur.close()
            if 'conn_mysql' in locals(): conn_mysql.close()

    conn_sqlite.close()