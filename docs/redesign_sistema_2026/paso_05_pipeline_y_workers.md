# Paso 05 - Pipeline y Workers

## Pipeline 1: Scraping

Nodos funcionales:

1. `scrape_job_created`
2. `source_precheck`
3. `source_running`
4. `source_completed | source_needs_human | source_failed`
5. `comments_persisted`

## Pipeline 2: Analysis

Nodos funcionales:

1. `analysis_job_created`
2. `comments_loaded`
3. `preprocess_done`
4. `llm_analysis_done`
5. `analysis_completed | analysis_failed`

## Worker responsibilities

### Worker `scraper-google-worker`

- Consume `scrape_jobs` con `source=google_maps`.
- Produce comentarios en `comments` con `source=google_maps`.
- Nunca evalua sesion TripAdvisor.

### Worker `scraper-tripadvisor-worker`

- Consume `scrape_jobs` con `source=tripadvisor`.
- Antes de ejecutar:
  - consulta `tripadvisor_session_state`.
- Si sesion invalida/expirada:
  - no scrapea;
  - marca `needs_human`.
- Si antibot durante scrape:
  - marca `needs_human`;
  - guarda diagnostico html y snippets.

### Worker `analysis-worker`

- Consume `analysis_jobs`.
- Carga comentarios por `scrape_job_ids`.
- Ejecuta preprocess + llm.
- Persiste resultado.

## Orquestador de workflow

Responsabilidades:

- crear `workflow` + `scrape_jobs`;
- consolidar estado global de workflow;
- habilitar `analysis` cuando haya datos validos.

Reglas minimas:

- si algun scrape esta `needs_human`, workflow `waiting_human`;
- si al menos un scrape `completed`, workflow `ready_for_analysis` (si no hay bloqueo explicito);
- si todo failed, workflow `failed`.

## Manejo de errores

- Errores recuperables:
  - `needs_human`;
  - timeout transitorio;
  - selector temporal.
- Errores no recuperables:
  - payload invalido;
  - bug de parser no tolerado;
  - datos criticos corruptos.

## Retry policy

- `max_attempts` por scrape job configurable.
- Backoff exponencial para fallos tecnicos.
- `needs_human` no cuenta como retry tecnico.

## Observabilidad

Cada transicion debe emitir evento con:

- `event_name`
- `workflow_id`
- `job_id`
- `source`
- `from_status`
- `to_status`
- `message`
- `data`
- `created_at`

## Definition of Done tecnica para pipeline

- Scrape y analyze operan por jobs separados.
- Comentarios persisten con `job_id` y `source`.
- TA usa estado global y soporta `needs_human`.
- Workflow refleja correctamente estado combinado.

