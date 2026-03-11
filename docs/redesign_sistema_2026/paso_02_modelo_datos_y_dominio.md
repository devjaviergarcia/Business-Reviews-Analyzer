# Paso 02 - Modelo de Datos y Dominio

## Entidades nuevas o redefinidas

## 1) `analysis_workflows`

Entidad padre de una ejecucion completa.

Campos propuestos:

- `_id`
- `workflow_name`
- `business_names.google_maps_name`
- `business_names.tripadvisor_name`
- `sources_requested` (`["google_maps"]`, `["tripadvisor"]`, `["google_maps","tripadvisor"]`)
- `status`
  - `pending_scrape`
  - `running_scrape`
  - `waiting_human`
  - `ready_for_analysis`
  - `running_analysis`
  - `completed`
  - `failed`
- `scrape_job_ids`
- `analysis_job_ids`
- `created_at`
- `updated_at`

## 2) `scrape_jobs`

Job de scraping por fuente.

Campos propuestos:

- `_id`
- `workflow_id`
- `job_type` (`scrape`)
- `source` (`google_maps` | `tripadvisor`)
- `business_name`
- `status`
  - `pending`
  - `running`
  - `needs_human`
  - `completed`
  - `failed`
- `attempts`
- `last_error`
- `requires_human_reason`
- `raw_output_path` (opcional)
- `metrics.scraped_comments_count`
- `metrics.duration_seconds`
- `started_at`
- `finished_at`
- `created_at`
- `updated_at`

## 3) `analysis_jobs`

Job de analisis de comentarios.

Campos propuestos:

- `_id`
- `workflow_id`
- `job_type` (`analysis`)
- `status` (`pending` | `running` | `completed` | `failed`)
- `scrape_job_ids` (fuentes seleccionadas)
- `analysis_config`
  - `batchers`
  - `batch_size`
  - `max_reviews_pool`
- `result_output_path` (opcional)
- `summary`
- `started_at`
- `finished_at`
- `created_at`
- `updated_at`

## 4) `comments`

Entidad canonica para reseñas/comentarios scrapeados.

Regla clave pedida:

- Todo comentario pertenece a un `job_id`.
- Todo comentario tiene `source`.

Campos propuestos:

- `_id`
- `workflow_id`
- `job_id` (scrape_job_id)
- `source` (`google_maps` | `tripadvisor`)
- `business_name`
- `external_comment_id` (si existe)
- `author_name`
- `rating`
- `relative_time`
- `written_date`
- `text`
- `owner_reply`
- `language`
- `raw_payload` (opcional o referencia)
- `fingerprint` (dedupe)
- `scraped_at`
- `created_at`
- `updated_at`

## 5) `tripadvisor_session_state`

Estado global operativo del worker TripAdvisor.

Campos propuestos:

- `_id` (`global_tripadvisor_session`)
- `session_state` (`valid` | `invalid` | `expired`)
- `availability_now` (bool)
- `last_human_intervention_at`
- `session_cookie_expires_at`
- `playwright_profile_path`
- `playwright_storage_state_path`
- `last_validation_attempt_at`
- `last_validation_result`
- `last_error`
- `bot_detected_count`
- `created_at`
- `updated_at`

## Relacion entre entidades

- `analysis_workflows` 1 -> N `scrape_jobs`
- `analysis_workflows` 1 -> N `analysis_jobs`
- `scrape_jobs` 1 -> N `comments`
- `analysis_jobs` N -> N `scrape_jobs` (via `scrape_job_ids`)

## Indices Mongo recomendados

- `scrape_jobs`:
  - `{status: 1, source: 1, updated_at: -1}`
  - `{workflow_id: 1, source: 1}`
- `analysis_jobs`:
  - `{status: 1, updated_at: -1}`
  - `{workflow_id: 1}`
- `comments`:
  - `{job_id: 1, source: 1}`
  - `{workflow_id: 1, source: 1}`
  - `{fingerprint: 1, job_id: 1}` unique parcial
- `tripadvisor_session_state`:
  - `_id` unico fijo

## Compatibilidad con modelo existente

- `reviews` actual puede convivir temporalmente.
- Migracion incremental:
  - Fase 1: dual-write (`reviews` + `comments`).
  - Fase 2: lectura primaria desde `comments`.
  - Fase 3: deprecacion de rutas antiguas.

