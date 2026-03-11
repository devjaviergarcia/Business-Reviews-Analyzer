# Paso 03 - Estado Global TripAdvisor

## Objetivo

Tener un estado global en base de datos que responda:

- cuando fue la ultima intervencion humana;
- cuando expira la cookie de sesion;
- si la sesion esta disponible ahora.

## Documento global (single source of truth)

Coleccion: `tripadvisor_session_state`

Documento fijo:

```json
{
  "_id": "global_tripadvisor_session",
  "session_state": "valid",
  "availability_now": true,
  "last_human_intervention_at": "2026-03-11T20:10:21Z",
  "session_cookie_expires_at": "2026-03-12T08:00:00Z",
  "playwright_profile_path": "playwright-data-tripadvisor",
  "playwright_storage_state_path": "playwright-data-tripadvisor/storage_state.json",
  "last_validation_attempt_at": "2026-03-11T20:30:00Z",
  "last_validation_result": "ok",
  "last_error": null,
  "bot_detected_count": 2,
  "created_at": "2026-03-11T20:00:00Z",
  "updated_at": "2026-03-11T20:30:00Z"
}
```

## Obtencion de expiracion de cookie desde Playwright

Fuente:

- `storage_state()` de Playwright (archivo JSON o payload en memoria).

Regla:

1. Leer `cookies[]`.
2. Filtrar cookies cuyo `domain` contenga `tripadvisor.`.
3. Tomar cookies con `expires > 0`.
4. `session_cookie_expires_at = max(expires)` convertido a UTC.
5. Si no hay cookies validas, marcar `session_state = invalid`.

## Algoritmo de decision antes de ejecutar scrape TripAdvisor

1. Cargar `tripadvisor_session_state`.
2. Si no existe -> crear con `invalid`.
3. Si `session_state = valid` y `session_cookie_expires_at > now`:
  - permitir ejecucion automatica.
4. Si `session_state = invalid` o `expired`:
  - `scrape_job.status = needs_human`.
5. Si durante ejecucion aparece antibot:
  - `scrape_job.status = needs_human`;
  - incrementar `bot_detected_count`;
  - guardar `last_error` y diagnostico HTML.

## Flujo manual (operador)

1. Operador recibe alerta `needs_human`.
2. Lanza navegador manual (headed, sin xvfb).
3. Completa verificacion/login.
4. Pulsa "Sesion guardada".
5. Sistema:
  - ejecuta `storage_state()`;
  - recalcula expiracion;
  - actualiza `tripadvisor_session_state`;
  - relanza scrape jobs de TripAdvisor en `needs_human`.

## Transiciones de estado de sesion

- `invalid -> valid`: intervencion humana correcta y cookie vigente.
- `valid -> expired`: `session_cookie_expires_at <= now`.
- `valid -> invalid`: fallo de validacion fuerte o cookie ausente.
- `expired -> valid`: nueva intervencion humana.

## Endpoints recomendados para este modulo

- `GET /tripadvisor/session-state`
- `POST /tripadvisor/session-state/refresh-from-storage`
- `POST /tripadvisor/session-state/manual-confirm`
- `POST /tripadvisor/session-state/mark-invalid`

## Criterios de aceptacion

- El estado global existe y se actualiza en cada flujo manual.
- La expiracion se calcula automaticamente desde cookies Playwright.
- Un scrape de TA nunca corre automatico con estado `invalid/expired`.
- El sistema relanza automaticamente jobs `needs_human` tras confirmacion.

