# Paso 06 - Estrategia de Testing

## Objetivo

Cada implementacion debe tener:

- tests unitarios;
- tests de integracion (solo BD o modulo puntual);
- tests e2e (flujo real de scraper cuando aplique).

## Piramide de testing

- Unitarios: base mas amplia.
- Integracion: contratos entre capas y Mongo.
- E2E: validacion real de scraping y pipeline.

## 1) Unit tests (obligatorios por feature)

Casos minimos:

- validadores de payload `scrape` y `analyze`;
- normalizacion de `source` y estados;
- maquina de estados de `tripadvisor_session_state`;
- parseo de cookies Playwright a `session_cookie_expires_at`;
- reglas de workflow (`ready_for_analysis`, `waiting_human`, etc).

Directorio sugerido:

- `tests/unit/...`

## 2) Integracion BD (obligatorios por modulo de dominio)

Casos minimos:

- CRUD de `tripadvisor_session_state` y transiciones.
- upsert de `scrape_jobs`, `analysis_jobs`, `comments`.
- indices y queries por `job_id` + `source`.
- consolidacion de estado de workflow desde varios jobs.

Directorio sugerido:

- `tests/integration_db/...`

Requisito:

- No lanzar navegador.
- Solo Mongo y servicios/repositorios.

## 3) Integracion especifica TripAdvisor Session (sin E2E)

Caso pedido explicitamente:

- usar un `storage_state` existente (fixture real o sample persistido).
- extraer expiracion de cookie.
- actualizar estado global.
- validar `availability_now`.

Fixture sugerida:

- `tests/fixtures/playwright/tripadvisor_storage_state_valid.json`
- `tests/fixtures/playwright/tripadvisor_storage_state_expired.json`
- `tests/fixtures/playwright/tripadvisor_storage_state_invalid.json`

Tests:

- `test_tripadvisor_session_from_existing_storage_state_valid`
- `test_tripadvisor_session_from_existing_storage_state_expired`
- `test_tripadvisor_session_from_existing_storage_state_no_cookie`

## 4) E2E de scraping

Casos minimos:

- Google-only scrape job.
- TripAdvisor scrape job con sesion valida.
- TripAdvisor scrape job que entra en `needs_human`.
- Relanzamiento despues de intervencion manual.

Directorio sugerido:

- `tests/e2e/...`
- scripts operativos en `scripts/`.

## 5) E2E de pipeline completa

Flujo:

1. crear `scrape_jobs`;
2. completar al menos una fuente;
3. lanzar `analysis_job`;
4. comprobar resultado y estados finales.

## Matriz de cobertura minima por ticket

Cada ticket debe declarar:

- `UT`: nombre de tests unitarios.
- `IT-DB`: nombre de tests integracion BD.
- `E2E`: nombre del escenario e2e (si aplica).

No se considera completado un ticket sin esta matriz.

## Criterio de release

- Ningun test critico fallando.
- Cobertura de paths de estado:
  - `pending -> running -> completed`;
  - `pending -> running -> needs_human -> running -> completed`;
  - `pending -> running -> failed`.

