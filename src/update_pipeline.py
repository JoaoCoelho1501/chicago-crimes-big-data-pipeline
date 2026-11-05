import subprocess
import sys
from datetime import datetime

def run(script):
    print(f"\n{'='*50}")
    print(f"A correr: {script}")
    print(f"{'='*50}")
    result = subprocess.run(
        ["python3" if "bronze" in script else "spark-submit", script],
        capture_output=False
    )
    if result.returncode != 0:
        print(f"\n ERRO em {script}. Pipeline interrompido.")
        sys.exit(1)
    print(f"✓ {script} concluído!")

print(f"\nInício do pipeline: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

run("ingest_bronze_incremental.py")
run("silver_incremental.py")
run("gold_update.py")

print(f"\nPipeline completo! {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
