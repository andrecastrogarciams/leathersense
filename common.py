from __future__ import annotations

import os
import logging
from pathlib import Path
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler
import sqlite3

# -----------------------------
# Caminhos (Paths)
# -----------------------------
# Define a raiz do projeto e garante que os dados/logs fiquem em locais previsíveis
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

# Caminho do banco SQLite, priorizando variável de ambiente se existir
SQLITE_PATH = Path(os.getenv("SQLITE_PATH", str(DATA_DIR / "monitoramento.db")))

# Define se a string de data salva no banco será em UTC ou Horário Local
# Recomendado manter UTC para evitar problemas com fuso horário/horário de verão
TS_MODE = os.getenv("TS_MODE", "UTC").upper().strip()  # UTC | LOCAL


# -----------------------------
# Logging (Registros do Sistema)
# -----------------------------
def setup_dirs() -> None:
    """Cria as pastas de dados e logs caso elas não existam."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logger(name: str, logfile_name: str) -> logging.Logger:
    """
    Configura um logger que escreve tanto no console quanto em um arquivo rotativo.
    O arquivo rotativo muda à meia-noite e mantém o histórico por 14 dias.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    log_file = LOG_DIR / logfile_name
    
    # Handler para rotacionar o log (evita que o arquivo cresça infinitamente)
    handler = TimedRotatingFileHandler(
        filename=str(log_file),
        when="midnight",
        backupCount=14,
        encoding="utf-8",
        utc=True,
    )
    
    # Formato: 2023-10-27T10:00:00Z | INFO | Mensagem
    formatter = logging.Formatter(
        fmt="%(asctime)sZ | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(formatter)

    # Evita duplicar handlers se a função for chamada múltiplas vezes
    if not logger.handlers:
        logger.addHandler(handler)
        # Adiciona saída visual no terminal
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)

    return logger


# -----------------------------
# SQLite (Banco de Dados Local)
# -----------------------------
def sqlite_connect() -> sqlite3.Connection:
    """
    Estabelece conexão com SQLite usando PRAGMAs de performance.
    WAL (Write-Ahead Logging) permite que o coletor escreva e o sincronizador leia 
    ao mesmo tempo sem travar o banco.
    """
    conn = sqlite3.connect(str(SQLITE_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")      # Performance e concorrência
    conn.execute("PRAGMA synchronous=NORMAL;")   # Melhora velocidade de escrita
    conn.execute("PRAGMA temp_store=MEMORY;")    # Tabelas temporárias em RAM
    conn.execute("PRAGMA foreign_keys=ON;")      # Garante integridade referencial
    return conn


def init_sqlite_queue(conn: sqlite3.Connection) -> None:
    """
    Cria a tabela de fila de leituras e seus índices.
    Os índices 'synced' e 'dead' são cruciais para que o sincronizador 
    seja rápido ao buscar o que ainda não foi enviado.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS queue_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reading_uuid TEXT NOT NULL UNIQUE,

            device_key TEXT NOT NULL,
            sensor_type TEXT NOT NULL,
            pin TEXT NOT NULL,

            ts_epoch_utc INTEGER NOT NULL, -- Unix timestamp (ordenação fácil)
            ts_utc TEXT NOT NULL,          -- String formatada para leitura humana

            temperature_c REAL,
            humidity_pct REAL,
            ok INTEGER NOT NULL DEFAULT 1, -- Indica se a leitura física deu certo
            error_msg TEXT,

            synced INTEGER NOT NULL DEFAULT 0, -- 0=Pendente, 1=Enviado
            dead INTEGER NOT NULL DEFAULT 0,   -- 1=Falha crítica/Ignorar
            attempts INTEGER NOT NULL DEFAULT 0, -- Contador de tentativas de envio
            last_attempt_utc TEXT
        );
        """
    )

    # Índices para otimizar buscas do sincronizador e relatórios
    conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_sync ON queue_readings(synced, dead, ts_epoch_utc);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_ts ON queue_readings(ts_epoch_utc);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_sensor ON queue_readings(device_key, sensor_type, pin, ts_epoch_utc);")
    conn.commit()


# -----------------------------
# Helpers (Utilitários)
# -----------------------------
def now_ts() -> tuple[int, str]:
    """
    Gera o tempo atual em dois formatos.
    Retorna:
      - Unix Epoch (ex: 1698412800) -> Bom para cálculos e filtros
      - String Formatada -> Bom para logs e visualização humana
    """
    epoch_utc = int(datetime.now(timezone.utc).timestamp())
    if TS_MODE == "LOCAL":
        ts_str = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
    else:
        ts_str = datetime.fromtimestamp(epoch_utc, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return epoch_utc, ts_str


def pin_for_db(dht_gpio: str) -> str:
    """
    Padroniza a nomenclatura de pinos.
    Converte formatos simples como 'D4' para o padrão 'GPIO4', 
    facilitando a identificação em dashboards.
    """
    s = (dht_gpio or "").strip().upper()
    if s.startswith("GPIO"):
        return s
    if s.startswith("D") and s[1:].isdigit():
        return f"GPIO{s[1:]}"
    return s


def get_local_ip() -> str:
    """
    Descobre o endereço IP interno do dispositivo (Raspberry Pi).
    Útil para telemetria, permitindo encontrar o dispositivo na rede local.
    """
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Não envia dados, apenas abre conexão para o SO definir a interface de saída
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return ""