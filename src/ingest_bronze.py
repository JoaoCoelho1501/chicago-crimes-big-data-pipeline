import os
HDFS_ROOT = os.environ.get("HDFS_ROOT", "hdfs://hadoop-master:39000/<utilizador>")
import requests
import json
import os
import subprocess
from collections import defaultdict

API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
START_DATE = "2022-01-01T00:00:00"
LIMIT = 50000
HDFS_BASE = f"{HDFS_ROOT}/bronze"
TMP_DIR = "/tmp/chicago_crimes_by_year"

os.makedirs(TMP_DIR, exist_ok=True)

records_by_year = defaultdict(list)
offset = 0

print("Iniciando ingestão...")

while True:
    params = {
        "$limit": LIMIT,
        "$offset": offset,
        "$where": f"date >= '{START_DATE}'"
    }
    response = requests.get(API_URL, params=params, timeout=60)
    batch = response.json()

    if not batch:
        print("Ingestão completa.")
        break

    for record in batch:
        year = record.get("year", "unknown")
        records_by_year[year].append(record)

    total = sum(len(v) for v in records_by_year.values())
    print(f"  offset={offset} | batch={len(batch)} | total={total}")
    offset += LIMIT

print(f"\nAnos encontrados: {sorted(records_by_year.keys())}")

for year, records in sorted(records_by_year.items()):
    tmp_path = f"{TMP_DIR}/chicago_crimes_{year}.json"
    hdfs_dir = f"{HDFS_BASE}/year={year}"
    hdfs_path = f"{hdfs_dir}/chicago_crimes.json"

    with open(tmp_path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    print(f"\nAno {year}: {len(records)} registos")
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
    subprocess.run(["hdfs", "dfs", "-put", "-f", tmp_path, hdfs_path], check=True)
    print(f"  ✓ HDFS: {hdfs_path}")

print("\nTudo concluído!")
