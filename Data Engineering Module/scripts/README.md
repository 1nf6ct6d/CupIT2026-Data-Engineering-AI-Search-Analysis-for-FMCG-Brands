# Data Engineering Module

## Логика пайплайна

Поток данных организован по слоям:

`API / text files -> raw -> stage -> dwh -> enrichment`


## Подготовка окружения

### 1. Установить зависимости

Из каталога `scripts`:

```powershell
python -m pip install -r requirements.txt
```


## Запуск пайплайна

### Полный rebuild

Очищает рабочие таблицы и запускает весь pipeline заново.

```powershell
python -m src.orchestration.run_full_rebuild_pipeline
```

Использовать, когда нужно полностью пересобрать `raw/stage/dwh` для текущего набора данных.

### Полный инкрементальный прогон

Запускает все шаги без предварительного `TRUNCATE`.

```powershell
python -m src.orchestration.run_full_incremental_pipeline
```

Подходит для регулярного дозапуска пайплайна поверх уже существующих данных.

### Только AI-ветка

Используется, если поисковая выдача уже собрана и нужно пересчитать только AI-часть:

```powershell
python -m src.orchestration.run_ai_pipeline
```

## Структура проекта

```text
scripts/
|-- README.md
|-- requirements.txt
|-- data/
|   `-- queries/
|       |-- search_queries.txt
|       `-- ai_queries.txt
|-- sql/
|   `-- checks/
|       `-- ai_pipeline_checks.sql
`-- src/
    |-- common/
    |   |-- config.py
    |   |-- db.py
    |   |-- utils.py
    |   `-- yandex_gpt_client.py
    |-- ingestion/
    |   |-- collect_search_results_to_json.py
    |   |-- load_raw_search_manual.py
    |   |-- generate_ai_from_search_results.py
    |   |-- load_raw_ai_manual.py
    |   |-- load_raw_test.py
    |   `-- load_raw_ai_test.py
    |-- parsing/
    |   |-- parse_raw_to_stage.py
    |   `-- parse_raw_ai_to_stage.py
    |-- dwh_loading/
    |   |-- load_stage_to_dwh.py
    |   `-- load_ai_stage_to_dwh.py
    |-- enrichment/
    |   |-- detect_brand_mentions.py
    |   |-- detect_ai_brand_mentions.py
    |   |-- classify_query_intent.py
    |   `-- classify_query_category.py
    `-- orchestration/
        |-- run_full_pipeline.py
        |-- run_full_incremental_pipeline.py
        |-- run_full_rebuild_pipeline.py
        `-- run_ai_pipeline.py
```




