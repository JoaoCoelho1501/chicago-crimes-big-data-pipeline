import requests
import json
import os

BASE_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"

# Ajusta aqui o período
START_DATE = "2022-01-01T00:00:00"

LIMIT = 50000
offset = 0

local_file = "/tmp/chicago_crimes_recent.json"

total = 0

with open(local_file, "w") as f:
    while True:
        params = {
            "$limit": LIMIT,
            "$offset": offset,
            "$where": f"date >= '{START_DATE}'",
            "$order": "date"
        }

        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()

        records = response.json()

        if not records:
            break

        for record in records:
            f.write(json.dumps(record) + "\n")

        total += len(records)
        print(f"Carregados {total} registros...")

        offset += LIMIT

print(f"Total final guardado localmente: {total}")

os.system(
    "hdfs dfs -fs hdfs://hadoop-master:39000 "
    "-put -f /tmp/chicago_crimes_recent.json /<utilizador>/bronze/"
)

print("Arquivo enviado para o Bronze no HDFS.")
