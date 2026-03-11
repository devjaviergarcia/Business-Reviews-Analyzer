# Fase 1 - Arquitectura modular y workers (plan de sprint ejecutable)

- `fase base`: `docs/backlogs/fase_01_arquitectura_modular_y_workers_backlog.md`
- `objetivo`: convertir el backlog de Fase 1 en una secuencia real de implementación sobre el código actual
- `alcance`: refactor incremental sin romper endpoints actuales (`/business/analyze`, queue, SSE)
- `estrategia`: contratos -> separación interna -> workers -> broker abstraction -> RabbitMQ ready

## Scope Decision (March 2026)

RabbitMQ is explicitly deferred for now.

- `backend activo`: `mongo` broker
- `deferred`: implementación operativa de `rabbitmq_broker`
- `se mantiene en fase 1`: como pendiente documentado (no se ejecuta en este ciclo)

## Diagnóstico del estado actual (código real)

### Punto de partida

1. `src/services/business_service.py` concentra demasiadas responsabilidades:
   - scraping (`_scrape_business_page`)
   - preprocess + LLM (`analyze_business`)
   - persistencia Mongo (`_upsert_reviews`)
   - jobs queue/read (`enqueue_business_analysis_job`, `get_business_analysis_job`, `list_business_analysis_jobs`)
   - query endpoints (`get_business`, `list_businesses`, `get_business_reviews`, `get_business_analysis`, `list_business_analyses`)
   - reanalyze y batching (`reanalyze_business_from_stored_reviews`, helpers)
2. `src/workers/scraper_worker.py` existe, pero ejecuta el flujo completo llamando directamente a `BusinessService.analyze_business(...)`.
3. No existen todavía `analysis-worker` ni `report-worker` reales.
4. `src/routers/business.py` y `src/routers/analysis.py` instancian `BusinessService()` por request.
5. `src/config.py` tiene config de worker básica (`worker_poll_seconds`) pero no configuración por tipo de worker ni broker abstraction.

### Implicación práctica

La Fase 1 no debe intentar “rehacer” todo. Debe:

1. Mantener la API estable.
2. Extraer contratos y capas internas primero.
3. Mover ejecución de workers a piezas especializadas con backend de cola actual.
4. Preparar la migración a RabbitMQ sin forzarla todavía.

## Objetivo concreto de esta fase (definición operativa)

Al terminar esta fase, el sistema debe poder:

1. Ejecutar el mismo flujo actual con endpoints compatibles.
2. Tener contratos internos de tareas/eventos (`scrape`, `analysis`, `report`) tipados.
3. Tener workers separados (`scrape-worker`, `analysis-worker`, `report-worker`) aunque inicialmente usen la cola actual.
4. Poder cambiar el backend de cola mediante una abstracción (primero Mongo/local, luego RabbitMQ).

## Enfoque de ejecución (sprints)

### Sprints propuestos

1. `Sprint 1`: Contratos, estados de job y DTOs internos.
2. `Sprint 2`: Separación de servicios/casos de uso y puertos/adaptadores.
3. `Sprint 3`: Workers especializados + colas lógicas sobre backend actual.
4. `Sprint 4`: Broker abstraction + preparación RabbitMQ (sin migración total).

### Cadencia sugerida

- Si trabajas solo: `1 sprint = 4-6 días efectivos`.
- Si hay equipo pequeño (2 personas): `1 sprint = 3-5 días`.

## Mapa de trabajo por archivos (orden recomendado)

### Archivos a refactorizar primero

1. `src/services/business_service.py`
2. `src/workers/scraper_worker.py`
3. `src/config.py`
4. `src/routers/business.py`
5. `src/routers/analysis.py`

### Archivos nuevos esperados (propuestos)

1. `src/workers/contracts.py`
2. `src/workers/events.py`
3. `src/workers/broker.py`
4. `src/workers/mongo_broker.py`
5. `src/workers/analysis_worker.py`
6. `src/workers/report_worker.py`
7. `src/services/business_query_service.py`
8. `src/services/analysis_job_service.py`
9. `src/services/analyze_business_use_case.py`
10. `src/services/reanalyze_use_case.py`
11. `src/services/ports.py` (o `src/domain/ports.py` si prefieres adelantar la estructura futura)
12. `src/services/adapters/google_maps_connector.py` (wrapper del scraper actual)
13. `src/services/adapters/llm_provider.py` / `src/services/adapters/gemini_provider.py`

Nota: los nombres exactos pueden variar, pero el objetivo es separar responsabilidades, no mover archivos “por mover”.

## Plan de sprint detallado (con estimación y owner sugerido)

### Sprint 1 - Contratos y estados de job (base de workers)

Objetivo: definir el lenguaje interno del pipeline sin romper nada.

#### Tickets priorizados

1. `T01-001` - Definir contrato de entrada/salida de la fase
   - `prioridad real`: `P0`
   - `estimacion`: `0.5 - 1 día`
   - `owner sugerido`: `backend-core`
   - `archivos`: `docs/backlogs/fase_01_arquitectura_modular_y_workers_backlog.md`, `docs/context/architecture/README.md`
   - `salida concreta`: schema de comandos/eventos v1 y ejemplo JSON por etapa

2. `T01-002` - Diseñar plan de pruebas/smoke de la fase
   - `prioridad real`: `P1`
   - `estimacion`: `0.5 día`
   - `owner sugerido`: `backend-core` / `qa-devex`
   - `archivos`: `docs/backlogs/fase_01_arquitectura_modular_y_workers_backlog.md`, `scripts/` (referencias de comandos)
   - `salida concreta`: checklist de smoke para refactor no-regresivo

3. `T01-003` - Documentar supuestos, límites y riesgos
   - `prioridad real`: `P1`
   - `estimacion`: `0.25 - 0.5 día`
   - `owner sugerido`: `backend-core`
   - `archivos`: `docs/context/phases/fase_01_arquitectura_modular_y_workers.md`
   - `salida concreta`: decisiones explícitas (sin RabbitMQ aún, API estable, migración incremental)

4. `T01-004` (parcial adelantado) - Crear DTOs/modelos internos de payloads de workers
   - `prioridad real`: `P0`
   - `estimacion`: `1 - 1.5 días`
   - `owner sugerido`: `backend-core`
   - `archivos`: `src/workers/contracts.py` (nuevo), `src/workers/events.py` (nuevo)
   - `salida concreta`:
     - `ScrapeTaskPayload`
     - `AnalysisTaskPayload`
     - `ReportTaskPayload`
     - `JobProgressEvent`
     - `TaskResult` (o resultados específicos)

5. `T01-007` (parcial adelantado) - Estados de job persistidos por etapa
   - `prioridad real`: `P0`
   - `estimacion`: `1 día`
   - `owner sugerido`: `backend-core`
   - `archivos`: `src/services/business_service.py` (temporal), luego `src/services/analysis_job_service.py`
   - `salida concreta`: normalización de `status/progress/events` con estructura consistente

#### Resultado esperado Sprint 1

1. Contratos internos definidos y usados en al menos un punto del worker.
2. Estados/eventos de job homogéneos.
3. Criterio de validación de Fase 1 fijado antes del refactor grande.

---

### Sprint 2 - Separación de servicios y puertos/adaptadores

Objetivo: romper el acoplamiento interno de `BusinessService` sin cambiar endpoints.

#### Tickets priorizados

1. `T01-004` - Separar query services, orchestration y analyze use cases
   - `prioridad real`: `P0`
   - `estimacion`: `2 - 3 días`
   - `owner sugerido`: `backend-core`
   - `archivos`:
     - `src/services/business_service.py` (recorte progresivo)
     - `src/services/business_query_service.py` (nuevo)
     - `src/services/analysis_job_service.py` (nuevo)
     - `src/services/analyze_business_use_case.py` (nuevo)
     - `src/services/reanalyze_use_case.py` (nuevo)
   - `salida concreta`:
     - `BusinessService` pasa a ser fachada fina o se reemplaza por composición
     - los routers dejan de depender de lógica monolítica directamente

2. `T01-005` - Introducir puertos para scraper y proveedor LLM
   - `prioridad real`: `P0`
   - `estimacion`: `1.5 - 2 días`
   - `owner sugerido`: `backend-core`
   - `archivos`:
     - `src/services/ports.py` (nuevo)
     - `src/services/adapters/google_maps_connector.py` (nuevo)
     - `src/services/adapters/gemini_provider.py` (nuevo)
     - `src/pipeline/llm_analyzer.py` (adaptación mínima)
     - `src/scraper/google_maps.py` (sin cambios de comportamiento, solo integración)
   - `salida concreta`:
     - `GoogleMapsScraper` y `ReviewLLMAnalyzer` quedan detrás de interfaces

3. `T01-006` - Documentar guía de extensión para nuevas fuentes
   - `prioridad real`: `P2` (pero cerrar antes de Fase 3)
   - `estimacion`: `0.5 día`
   - `owner sugerido`: `backend-core`
   - `archivos`: `docs/context/architecture/README.md`, `docs/context/phases/fase_03_scraper_tripadvisor.md`
   - `salida concreta`: checklist “cómo añadir un source”

4. `T01-006` (integración routers, derivado de T01-004)
   - `prioridad real`: `P1`
   - `estimacion`: `0.5 - 1 día`
   - `owner sugerido`: `backend-core`
   - `archivos`:
     - `src/routers/business.py`
     - `src/routers/analysis.py`
   - `salida concreta`:
     - routers usan servicios especializados/fachada limpia
     - sin cambiar contrato HTTP

#### Resultado esperado Sprint 2

1. `BusinessService` deja de ser el punto único de todo.
2. Scraper y LLM se consumen vía puertos/adaptadores.
3. La API sigue funcionando igual desde fuera.

---

### Sprint 3 - Workers especializados y colas lógicas (backend actual)

Objetivo: separar ejecución por tipo de carga sin RabbitMQ todavía.

#### Tickets priorizados

1. `T01-007` - Crear `scrape-worker`, `analysis-worker`, `report-worker`
   - `prioridad real`: `P0`
   - `estimacion`: `2 - 3 días`
   - `owner sugerido`: `backend-core` / `backend-infra`
   - `archivos`:
     - `src/workers/scraper_worker.py` (refactor)
     - `src/workers/analysis_worker.py` (nuevo)
     - `src/workers/report_worker.py` (nuevo)
     - `src/workers/__init__.py`
   - `salida concreta`:
     - cada worker con loop propio y handler específico
     - `scraper_worker` deja de ejecutar “todo”

2. `T01-008` - Introducir `queue_name` / `routing_key` internos
   - `prioridad real`: `P0`
   - `estimacion`: `1 - 1.5 días`
   - `owner sugerido`: `backend-infra`
   - `archivos`:
     - `src/services/analysis_job_service.py`
     - `src/workers/contracts.py`
     - `src/workers/scraper_worker.py`
     - `src/workers/analysis_worker.py`
     - `src/workers/report_worker.py`
   - `salida concreta`:
     - jobs/subjobs con cola lógica (`scrape`, `analysis`, `report`)
     - dispatch por etapa/cola

3. `T01-009` - Concurrencia y timeouts por worker
   - `prioridad real`: `P1`
   - `estimacion`: `1 día`
   - `owner sugerido`: `backend-infra`
   - `archivos`:
     - `src/config.py`
     - `src/workers/*.py`
   - `salida concreta`:
     - `worker_scrape_poll_seconds`, `worker_analysis_poll_seconds`, etc. (o un bloque por worker)
     - límites de concurrencia por tipo

4. `T01-008/T01-009` (observabilidad asociada)
   - `prioridad real`: `P1`
   - `estimacion`: `0.5 - 1 día`
   - `owner sugerido`: `backend-core`
   - `archivos`:
     - `src/routers/business.py` (SSE sigue intacto)
     - `src/services/analysis_job_service.py`
   - `salida concreta`:
     - SSE refleja etapa real (`scrape`, `analysis`, `report`)

#### Resultado esperado Sprint 3

1. Existen 3 workers ejecutables.
2. El trabajo está separado por colas lógicas.
3. El pipeline sigue funcionando con backend actual.

---

### Sprint 4 - Broker abstraction y preparación RabbitMQ (sin migración total)

Objetivo: dejar la arquitectura lista para activar RabbitMQ por configuración.

Estado en este ciclo: `DEFERRED` (documentado, no implementado).

#### Tickets priorizados

1. `T01-010` - Diseñar topología RabbitMQ (exchanges/queues/DLQ)
   - `prioridad real`: `P1`
   - `estimacion`: `0.5 - 1 día`
   - `owner sugerido`: `backend-infra`
   - `archivos`: `docs/context/architecture/README.md`, `docs/backlogs/fase_01_arquitectura_modular_y_workers_backlog.md`
   - `salida concreta`: topología v1 documentada (`commands`, `events`, colas por worker)

2. `T01-011` - Crear abstracción de broker y adaptadores
   - `prioridad real`: `P0`
   - `estimacion`: `2 - 3 días`
   - `owner sugerido`: `backend-infra`
   - `archivos`:
     - `src/workers/broker.py` (nuevo)
     - `src/workers/mongo_broker.py` (nuevo)
     - `src/workers/rabbitmq_broker.py` (stub o implementación inicial)
     - `src/workers/*.py`
     - `src/config.py`
   - `salida concreta`:
     - workers/orquestador consumen interfaz, no implementación concreta
     - backend actual sigue operativo

3. `T01-012` - Plan de migración incremental con rollback
   - `prioridad real`: `P1`
   - `estimacion`: `0.5 día`
   - `owner sugerido`: `backend-infra` / `backend-core`
   - `archivos`: `docs/context/phases/fase_01_arquitectura_modular_y_workers.md`, `docs/context/architecture/README.md`
   - `salida concreta`: hitos de migración a RabbitMQ con smoke tests por hito

4. `T01-010/T01-011` (soporte dev)
   - `prioridad real`: `P2`
   - `estimacion`: `0.5 - 1 día`
   - `owner sugerido`: `backend-infra`
   - `archivos`:
     - `docker-compose.yml`
     - `.env.example`
   - `salida concreta`:
     - RabbitMQ opcional por perfil/servicio
     - flags para usar broker actual vs RabbitMQ

#### Resultado esperado Sprint 4

1. La arquitectura ya no depende del backend de cola en el código de negocio.
2. RabbitMQ puede activarse por configuración (aunque todavía no sea la ruta por defecto).
3. Existe plan de migración con rollback.

## Priorización global (resumen ejecutivo)

### P0 (hacer primero)

1. Contratos internos de tasks/events (`T01-001`, DTOs internos).
2. Separación de `BusinessService` en orquestación / query / use cases (`T01-004`).
3. Puertos/adaptadores para scraper y LLM (`T01-005`).
4. Workers especializados (`T01-007`).
5. Broker abstraction (`T01-011`).

### P1 (debe entrar en Fase 1)

1. Plan de pruebas/smoke (`T01-002`).
2. Estados homogéneos y eventos por etapa.
3. Colas lógicas y concurrencia/timeouts (`T01-008`, `T01-009`).
4. Topología RabbitMQ + plan migración (`T01-010`, `T01-012`).

### P2 (puede cerrar al final de fase si falta tiempo)

1. Guía de extensión para nuevas fuentes (`T01-006`).
2. Ajustes de compose/dev para RabbitMQ opcional (si el stub ya existe).

## Riesgos reales y mitigaciones (sobre tu código actual)

1. Riesgo: romper endpoints al tocar `BusinessService`
   - Mitigación: mantener `BusinessService` como fachada temporal y mover lógica por delegación.

2. Riesgo: duplicación temporal de lógica entre workers
   - Mitigación: extraer handlers por etapa antes de crear más workers.

3. Riesgo: mezclar refactor arquitectónico con cambios funcionales del scraper
   - Mitigación: congelar comportamiento de Google Maps en Fase 1; cambios de scraping van a fases posteriores o bugfixs explícitos.

4. Riesgo: introducir RabbitMQ demasiado pronto
   - Mitigación: primero broker abstraction + cola actual; RabbitMQ como backend alternativo.

## Criterio de cierre de Fase 1 (medible)

La Fase 1 se considera cerrada cuando:

1. `POST /business/analyze`, `POST /business/analyze/queue` y SSE siguen funcionando.
2. Hay 3 workers separados ejecutables (`scrape`, `analysis`, `report`).
3. Existe `broker abstraction` y backend actual implementado detrás de ella.
4. Scraper y LLM se consumen mediante puertos/adaptadores.
5. La ruta de migración a RabbitMQ está documentada y validada con smoke tests definidos.

## Comandos recomendados para validar durante la fase

1. `uv run python -m compileall src scripts`
2. `uv run python scripts/generate_context_docs.py`
3. `uv run python scripts/generate_phase_backlogs.py`
4. Smoke de API local (`/business/analyze`, `/business/analyze/queue`, SSE)
5. Worker local (`uv run python -m src.workers.scraper_worker`) y luego futuros workers

## Nota de mantenimiento

Este archivo es **manual** (no lo genera `scripts/generate_phase_backlogs.py`) para que puedas ajustar:

- estimaciones
- owners
- orden real de sprint
- decisiones de arquitectura

sin que se sobrescriba al regenerar los backlogs base.
