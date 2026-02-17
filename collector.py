from __future__ import annotations

import os
import time
import uuid
import signal
import logging
from datetime import datetime, timezone

import board
import adafruit_dht

# Importações de utilitários locais do projeto (common.py)
from common import (
    setup_dirs,
    setup_logger,
    sqlite_connect,
    init_sqlite_queue,
    now_ts,
    pin_for_db,
    SQLITE_PATH,
)

# ---------------------------------------------------------
# Configurações via Variáveis de Ambiente (ENV)
# ---------------------------------------------------------
# Identificação única do dispositivo (ex: Raspberry Pi 1)
DEVICE_KEY = os.getenv("DEVICE_KEY", "raspi-unknown")

# Intervalos de leitura e controle de retry
READ_INTERVAL_SECONDS = int(os.getenv("READ_INTERVAL_SECONDS", "10"))
READ_RETRY_SLEEP_SECONDS = float(os.getenv("READ_RETRY_SLEEP_SECONDS", "2.0"))
MAX_CONSECUTIVE_READ_ERRORS = int(os.getenv("MAX_CONSECUTIVE_READ_ERRORS", "10"))

# Configurações físicas do sensor DHT
DHT_GPIO = os.getenv("DHT_GPIO", "D4")  # Pino de dados (ex: D4 para GPIO4)
DHT_TYPE = os.getenv("DHT_TYPE", "DHT11").upper().strip()  # Modelo: DHT11 ou DHT22
DHT_USE_PULSEIO = os.getenv("DHT_USE_PULSEIO", "0") == "1" # Lib gpiod vs pulseio

SENSOR_TYPE = DHT_TYPE
SENSOR_PIN = os.getenv("SENSOR_PIN", pin_for_db(DHT_GPIO))  # Formato para o DB


# ---------------------------------------------------------
# Persistência no SQLite
# ---------------------------------------------------------
def insert_queue(
    conn,
    logger: logging.Logger,
    reading_uuid: str,
    ts_epoch_utc: int,
    ts_utc: str,
    temperature_c: float | None,
    humidity_pct: float | None,
    ok: int,
    error_msg: str | None,
) -> None:
    """
    Insere uma leitura (com sucesso ou erro) na fila local do SQLite.
    Campos 'synced', 'dead' e 'attempts' são inicializados como 0 para o processo de sync.
    """
    conn.execute(
        """
        INSERT INTO queue_readings
        (reading_uuid, device_key, sensor_type, pin, ts_epoch_utc, ts_utc, temperature_c, humidity_pct, ok, error_msg,
         synced, dead, attempts)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0)
        """,
        (
            reading_uuid,
            DEVICE_KEY,
            SENSOR_TYPE,
            SENSOR_PIN,
            ts_epoch_utc,
            ts_utc,
            temperature_c,
            humidity_pct,
            ok,
            error_msg,
        ),
    )
    conn.commit()

    if ok:
        logger.info("Local salvo | uuid=%s | T=%.1f°C U=%.1f%%", reading_uuid, float(temperature_c), float(humidity_pct))
    else:
        logger.warning("Local falha | uuid=%s | erro=%s", reading_uuid, error_msg)


# ---------------------------------------------------------
# Fábrica do Sensor (Sensor Factory)
# ---------------------------------------------------------
def make_sensor(logger: logging.Logger):
    """
    Instancia o objeto do sensor DHT baseado no modelo (DHT11/DHT22) 
    definido nas variáveis de ambiente.
    """
    pin = getattr(board, DHT_GPIO)
    if DHT_TYPE == "DHT22":
        sensor = adafruit_dht.DHT22(pin, use_pulseio=DHT_USE_PULSEIO)
        logger.info("Sensor DHT22 iniciado em board.%s", DHT_GPIO)
    else:
        sensor = adafruit_dht.DHT11(pin, use_pulseio=DHT_USE_PULSEIO)
        logger.info("Sensor DHT11 iniciado em board.%s", DHT_GPIO)
    return sensor


# ---------------------------------------------------------
# Loop Principal e Tratamento de Sinais
# ---------------------------------------------------------
RUNNING = True

def handle_stop(signum, frame):
    """Interrompe o loop principal ao receber SIGINT (Ctrl+C) ou SIGTERM."""
    global RUNNING
    RUNNING = False


def main():
    # Inicialização de diretórios, logs e banco de dados
    setup_dirs()
    logger = setup_logger("leathersense_collector", "collector.log")

    # Configura os listeners de encerramento do sistema
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    # Conecta e garante que a tabela de fila existe
    conn_sqlite = sqlite_connect()
    init_sqlite_queue(conn_sqlite)
    logger.info("SQLite OK: %s", SQLITE_PATH)

    # Inicializa o sensor e o contador de erros consecutivos
    sensor = make_sensor(logger)
    consecutive_errors = 0

    logger.info(
        "Collector iniciado | device=%s | sensor_type=%s | pin=%s | intervalo=%ss | sqlite=%s",
        DEVICE_KEY,
        SENSOR_TYPE,
        SENSOR_PIN,
        READ_INTERVAL_SECONDS,
        SQLITE_PATH,
    )

    while RUNNING:
        rid = str(uuid.uuid4()) # ID único para cada tentativa de leitura
        ts_epoch_utc, ts_utc = now_ts() # Timestamp gerado via common.py

        try:
            # Tentativa de leitura física do sensor
            temp = sensor.temperature
            umid = sensor.humidity

            # O DHT às vezes retorna None mesmo sem lançar exceção
            if temp is None or umid is None:
                consecutive_errors += 1
                insert_queue(conn_sqlite, logger, rid, ts_epoch_utc, ts_utc, None, None, 0, "Leitura nula do DHT")
                time.sleep(READ_RETRY_SLEEP_SECONDS)
            else:
                # Sucesso: Reseta erros, salva no banco e aguarda o intervalo padrão
                consecutive_errors = 0
                insert_queue(conn_sqlite, logger, rid, ts_epoch_utc, ts_utc, float(temp), float(umid), 1, None)
                time.sleep(READ_INTERVAL_SECONDS)

        except RuntimeError as e:
            # Erros comuns de leitura do DHT (checksum, timing) - tenta novamente rápido
            consecutive_errors += 1
            insert_queue(conn_sqlite, logger, rid, ts_epoch_utc, ts_utc, None, None, 0, str(e))
            time.sleep(READ_RETRY_SLEEP_SECONDS)

        except Exception as e:
            # Erros críticos ou inesperados
            consecutive_errors += 1
            insert_queue(conn_sqlite, logger, rid, ts_epoch_utc, ts_utc, None, None, 0, f"Erro inesperado: {e}")
            time.sleep(READ_RETRY_SLEEP_SECONDS)

        # Lógica de auto-recuperação: se falhar muitas vezes, reinicia o objeto do sensor
        if consecutive_errors >= MAX_CONSECUTIVE_READ_ERRORS:
            logger.warning("Muitas falhas seguidas (%s). Reiniciando sensor...", consecutive_errors)
            try:
                sensor.exit()
            except Exception:
                pass
            sensor = make_sensor(logger)
            consecutive_errors = 0

    # --- Limpeza Final (Graceful Shutdown) ---
    logger.info("Encerrando collector...")
    try:
        sensor.exit()
    except Exception:
        pass
    try:
        conn_sqlite.close()
    except Exception:
        pass
    logger.info("Finalizado.")


if __name__ == "__main__":
    main()