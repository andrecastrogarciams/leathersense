# LeatherSense — Monitoramento (Raspberry Pi + DHT + SQLite + MySQL + Dashboard no Servidor)

Arquitetura com **dois serviços no Raspberry**:
- **Collector**: lê o sensor (DHT11/DHT22) e grava **localmente no SQLite** (fila/auditoria).
- **Sync**: pega a fila no SQLite e envia para o **MySQL na rede** (com retry/backoff).

A dashboard (Streamlit) roda **no servidor** (onde está o MySQL), não no Raspberry.

---

## 1) Visão geral (por que assim?)
- **Não perde dados** se cair rede/MySQL: o Raspberry continua salvando no SQLite.
- **Escala fácil**: vários raspberries/sensores mandando pro mesmo MySQL.
- **Menos dor**: leitura do sensor não depende de banco remoto.

---

## 2) Requisitos de hardware
- Raspberry Pi com Raspberry Pi OS
- Sensor DHT11 ou DHT22 (módulo ou sensor “cru”)
- Cabos jumpers
- (Se for sensor cru, sem plaquinha): resistor 4.7k ou 10k entre VCC e DATA

### Conexão sugerida (padrão)
- VCC → 3.3V (pino físico 1)
- GND → GND (pino físico 6)
- DATA → GPIO4 (pino físico 7)  → no código: `DHT_GPIO=D4`

---

## 3) Requisitos do sistema (Raspberry)
```bash
sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get install -y libgpiod2 python3-venv
```

> Dica: se for rodar como usuário `tap`, garanta acesso ao GPIO:
```bash
sudo usermod -aG gpio tap
sudo reboot
```

---

## 4) Instalação (Raspberry)
### 4.1) Criar ambiente virtual + instalar dependências
```bash
cd /home/tap/leathersense
python3 -m venv venv
source venv/bin/activate
pip install -U pip setuptools wheel
pip install -r requirements.txt
```

### 4.2) Configurar `.env`
Crie o arquivo `.env` (pode partir do `.env.example`):
```bash
cp .env.example .env
nano .env
```

Campos importantes:
- `DEVICE_KEY`: identificador único por Raspberry (ex: `rasplab-100`)
- `DHT_TYPE`: `DHT11` ou `DHT22`
- `DHT_GPIO`: pino do sensor (ex: `D4`)
- `MYSQL_*`: host/usuário/senha do MySQL (somente para o Sync)
- `MYSQL_SSL_DISABLED=1`: útil em rede interna com certificado self-signed

> **Não use SENSOR_ID fixo**. O Sync resolve automaticamente o sensor no MySQL por `(device_key + sensor_type + pin)`.

---

## 5) Rodar manualmente (teste rápido)
### 5.1) Collector (sensor → sqlite)
```bash
source venv/bin/activate
python collector.py
```

### 5.2) Sync (sqlite → mysql)
Em outro terminal:
```bash
source venv/bin/activate
python sync.py
```

Logs (também vão pro journal se estiver via systemd):
- `logs/collector.log`
- `logs/sync.log`

---

## 6) Rodar como serviço (systemd)
### 6.1) Instalar unidades
```bash
sudo cp systemd/leathersense_collector.service /etc/systemd/system/
sudo cp systemd/leathersense_sync.service /etc/systemd/system/
sudo systemctl daemon-reload
```

### 6.2) Ativar e iniciar
```bash
sudo systemctl enable leathersense_collector leathersense_sync
sudo systemctl restart leathersense_collector leathersense_sync
```

### 6.3) Ver status
```bash
sudo systemctl status leathersense_collector -l --no-pager
sudo systemctl status leathersense_sync -l --no-pager
```

### 6.4) Ver logs em tempo real
```bash
sudo journalctl -u leathersense_collector -f --no-pager
```

Em outro terminal:
```bash
sudo journalctl -u leathersense_sync -f --no-pager
```

---

## 7) SQLite: preciso criar o banco?
**Não.** O `collector.py` cria automaticamente:
- pasta `data/`
- arquivo `monitoramento.db`
- tabela `queue_readings`

Ou seja: em um Raspberry novo, basta rodar o collector uma vez (ou iniciar o serviço).

---

## 8) Migração (se você tinha a versão antiga com `readings_local`)
Se você já tem um SQLite antigo e quer migrar pro schema novo:
```bash
source venv/bin/activate
python tools/migrate_sqlite_v2.py
```

Ele cria a tabela nova (`queue_readings`), copia dados, e renomeia a antiga para `readings_local_legacy` (se existir).

---

## 9) Como o Sync identifica/cria device e sensor no MySQL
O Sync segue essa lógica:

1) **Device**
- garante `devices.device_key` (UPSERT)
- pega `devices.id`

2) **Sensor**
- procura sensor por `(device_id, sensor_type, pin)`
- se não existir, cria e pega `sensors.id`

3) **Readings**
- insere em `readings` com `sensor_id` correto (FK)
- recomendado ter `UNIQUE(reading_uuid)` para idempotência

---

## 10) MySQL: permissões mínimas (usuário ingest)
O usuário `leathersense_ingest` precisa:
- `SELECT/INSERT/UPDATE` em `devices` e `sensors` (bootstrap)
- `INSERT/UPDATE` em `readings` (ingest idempotente)

> Se você quiser, deixe `SELECT` também em `readings` (não é obrigatório, mas ajuda debug).

---

## 11) Dashboard (Streamlit) no servidor
A dashboard deve rodar no servidor (ex: `noaserverapp`) usando o MySQL como fonte.

Este repositório cobre a **coleta** e o **sync** no Raspberry.
A dashboard é outro projeto/pasta (ex: `leathersense_dash/`), com nginx/porta própria (ex: 8502).

---

## 12) Troubleshooting rápido
### DHT falhando muito (checksum/buffer)
- DHT11 é chatinho. Ajuste no `.env`:
  - `DHT_USE_PULSEIO=0` (default)
  - se continuar ruim, teste `DHT_USE_PULSEIO=1`

### Sync OFF
- Confira variáveis `MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASS`, `MYSQL_DB`
- Confira permissões do usuário no MySQL
- Veja `journalctl -u leathersense_sync -f`

### FK error (1452)
- Normalmente é schema MySQL inconsistente ou sensor não foi criado.
- Com esta arquitetura, o SQLite não guarda sensor_id, então tende a desaparecer.

---

## 13) Licença / Notas
Projeto interno VIPOSA/NOA/TAP.
