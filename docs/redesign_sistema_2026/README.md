# Rediseno Completo del Sistema (2026)

Este paquete define el rediseno end-to-end solicitado para:

- separar `scrape` y `analyze` en jobs distintos;
- introducir estado global de sesion TripAdvisor;
- modelar comentarios por `job_id` y `source`;
- formalizar pipeline/worker/API;
- definir backlog ejecutable con estrategia de testing;
- especificar UX/UI detallada del canvas tipo nodos.

## Indice

1. [Paso 01 - Objetivos y Principios](./paso_01_objetivos_y_principios.md)
2. [Paso 02 - Modelo de Datos y Dominio](./paso_02_modelo_datos_y_dominio.md)
3. [Paso 03 - Estado Global TripAdvisor](./paso_03_estado_global_tripadvisor.md)
4. [Paso 04 - Contratos API (Scrape vs Analyze)](./paso_04_contratos_api_scrape_analyze.md)
5. [Paso 05 - Pipeline y Workers](./paso_05_pipeline_y_workers.md)
6. [Paso 06 - Estrategia de Testing](./paso_06_estrategia_testing.md)
7. [Paso 07 - Epica e Historias de Usuario](./paso_07_epica_historias_tareas.md)
8. [Paso 08 - Especificacion UXUI de Pipeline](./paso_08_especificacion_uxui_pipeline.md)
9. [Paso 09 - Plan de Implementacion por Fases](./paso_09_plan_implementacion_fases.md)

## Alcance de este rediseno

- Orientado al repositorio actual (FastAPI + Mongo + workers + Playwright).
- Compatible con evolucion incremental (sin reescribir todo de cero).
- Prioriza robustez operativa de TripAdvisor y trazabilidad de jobs.

## Regla operativa principal

- `scrape_job` produce datos brutos por fuente (`google_maps` o `tripadvisor`).
- `analysis_job` consume comentarios ya scrapeados (una o varias fuentes).
- `tripadvisor_session_state` gobierna si TripAdvisor corre automatizado o pasa a `needs_human`.

