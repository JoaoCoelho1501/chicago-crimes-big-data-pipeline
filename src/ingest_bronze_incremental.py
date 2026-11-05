import os
HDFS_ROOT = os.environ.get("HDFS_ROOT", "hdfs://hadoop-master:39000/<utilizador>")
import requests
import json
import os
import subprocess
import time
from datetime import datetime, timedelta

API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
LIMIT = 50000
HDFS_BASE = f"{HDFS_ROOT}/bronze"
TMP_DIR = "/tmp/chicago_crimes_incremental"
LAST_UPDATE_FILE = os.path.expanduser("~/last_update.txt")

os.makedirs(TMP_DIR, exist_ok=True)

# ── Ler a data da última atualização ─────────────────────────────────
if os.path.exists(LAST_UPDATE_FILE):
    with open(LAST_UPDATE_FILE, "r") as f:
        last_update = f.read().strip()
    print(f"Última atualização: {last_update}")
else:
    # Se nunca correu, vai buscar os últimos 8 dias
    last_update = (datetime.now() - timedelta(days=8)).strftime("%Y-%m-%dT%H:%M:%S")
    print(f"Primeira execução — a buscar desde: {last_update}")

# ── Descarregar só os crimes novos ───────────────────────────────────
from collections import defaultdict
records_by_year = defaultdict(list)
offset = 0

print("A descarregar dados novos...")

while True:
    params = {
        "$limit": LIMIT,
        "$offset": offset,
        "$where": f"date > '{last_update}'"
    }

    for tentativa in range(5):
        try:
            response = requests.get(API_URL, params=params, timeout=120)
            batch = response.json()
            break
        except Exception as e:
            print(f"  Erro (tentativa {tentativa+1}/5): {e}")
            time.sleep(10)
    else:
        print("Falhou 5 vezes. A parar.")
        break

    if not batch:
        print("Sem dados novos.")
        break

    for record in batch:
        year = record.get("year", "unknown")
        records_by_year[year].append(record)

    total = sum(len(v) for v in records_by_year.values())
    print(f"  offset={offset} | batch={len(batch)} | total={total}")
    offset += LIMIT

if not records_by_year:
    print("Nenhum dado novo encontrado. Pipeline terminado.")
    exit(0)

print(f"\nAnos com dados novos: {sorted(records_by_year.keys())}")

# ── Adicionar ao Bronze (sem apagar o existente) ──────────────────────
for year, records in sorted(records_by_year.items()):
    tmp_path = f"{TMP_DIR}/chicago_crimes_{year}_new.json"
    hdfs_dir = f"{HDFS_BASE}/year={year}"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    hdfs_path = f"{hdfs_dir}/chicago_crimes_{timestamp}.json"

    # Guardar localmente
    with open(tmp_path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    print(f"\nAno {year}: {len(records)} registos novos")
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
    subprocess.run(["hdfs", "dfs", "-put", "-f", tmp_path, hdfs_path], check=True)
    print(f"  ✓ HDFS: {hdfs_path}")

# ── Guardar a data desta atualização ─────────────────────────────────
now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
with open(LAST_UPDATE_FILE, "w") as f:
    f.write(now)

print(f"\nBronze incremental concluído! Próxima atualização vai buscar desde: {now}")
