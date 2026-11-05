# Chicago Crimes: Pipeline de Big Data (Medallion Architecture)

> Pipeline Spark + HDFS que recolhe mais de 1 milhão de ocorrências criminais de Chicago (2022–2026) e as organiza em camadas Bronze, Silver e Gold para análise.

**Mestrado em Business Intelligence & Analytics (ISCAP)**, cadeira de Analítica para Big Data, 2025/26

## O problema
Que padrões de criminalidade aparecem quando se tratam, de forma sistemática, **1.077.035 ocorrências** de uma cidade como Chicago? Onde e a que horas acontecem os crimes, que tipos dominam e como evoluíram ao longo de quatro anos?

## Arquitetura

```
API Socrata (JSON, 50k registos/pedido)
        │  ingestão com paginação por offset
        ▼
 BRONZE  JSON Lines, em bruto, particionado por ano   (ingest_bronze.py)
        ▼
 SILVER  limpeza e tipagem, Parquet + Snappy          (silver.py)
        ▼
 GOLD    agregações analíticas                        (gold.py)
        ▼
 Visualizações (PySpark + seaborn)                    (notebooks/gold_visualizacoes.ipynb)
```

Também há uma **atualização incremental** (`*_incremental.py`, `gold_update.py`, `update_pipeline.py`), que só processa registos novos desde a última execução.

## Principais conclusões
- A criminalidade é **maioritariamente diurna**, não noturna.
- **Março e abril** têm mais crimes do que julho.
- A diferença entre dias da semana é quase nula.
- Guardar em Parquet com Snappy e particionar por ano torna as consultas muito mais rápidas do que sobre o JSON original.

## Stack
`PySpark` · `HDFS` · `Parquet` · `Python (requests)` · `matplotlib` / `seaborn` · `spark-submit`

## Estrutura
```
src/        scripts do pipeline (ingestão, silver, gold, incremental)
notebooks/  15 visualizações sobre a camada Gold
docs/       relatório completo (PDF)
```

## Como correr
```bash
export HDFS_ROOT="hdfs://<host>:<porta>/<utilizador>"
python src/chicago_crimes.py                # descarrega os dados da API
spark-submit src/ingest_bronze.py
spark-submit src/silver.py
spark-submit src/gold.py
# atualizações seguintes:
python src/update_pipeline.py
```
Dados: [City of Chicago Open Data – Crimes 2001 to Present](https://data.cityofchicago.org/resource/ijzp-q8t2.json)

## Autor
João Coelho
