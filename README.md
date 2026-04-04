# Checker

Dashboard Streamlit e collector batch per costi AWS e metriche pod, con supporto sia locale sia produzione.

## Modalita locali

- Avvio dashboard: `./run_app.sh`
- Avvio dashboard dopo refresh locale dei dati: `./run_app.sh --collect`

In locale `run_app.sh` mantiene il fallback a `aws sso login` se i collector falliscono per token AWS scaduto.

## Modalita produzione

La modalita consigliata ora usa un solo container:

- all'avvio la dashboard esegue `python main.py dashboard --allow-missing-remote-db`
- nello stesso container gira anche `cron`
- ogni giorno alle `05:00 UTC` il cron esegue `python main.py refresh-db --allow-missing-remote-db`

Se `DUCKDB_S3_URI` e' impostato, il runtime:

- scarica il file DuckDB da S3 prima di avviare la dashboard;
- sul refresh lavora su una working copy locale del DB;
- a refresh completato sostituisce in modo atomico il DB live nel container;
- carica il DB aggiornato su S3.

Variabili utili per questa modalita:

- `ENABLE_INTERNAL_CRON=1`: abilita il cron interno al container
- `CHECKER_CRON_SCHEDULE="0 5 * * *"`: schedule cron, default `05:00 UTC`

## Variabili ambiente principali

- `DUCKDB_DATABASE`: nome del file DuckDB. Default: `database.duckdb`
- `DUCKDB_PATH`: path locale assoluto del file DuckDB. Ha precedenza su `DUCKDB_DATABASE`
- `DUCKDB_LOCAL_DIR`: directory locale del file DuckDB se `DUCKDB_PATH` non e' impostato
- `DUCKDB_S3_URI`: URI S3 del file canonico, ad esempio `s3://my-bucket/checker/database.duckdb`
- `AWS_REGION` o `AWS_DEFAULT_REGION`: regione usata per S3/STS quando necessaria

Le credenziali AWS macchina-macchina in produzione devono essere fornite dal task role ECS, con `sts:AssumeRole` verso i ruoli read-only cross-account necessari ai collector.
