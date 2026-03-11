# Paso 07 - Epica e Historias de Usuario

## Epica principal

`E26-01` - Rediseno operacional de pipeline Scrape/Analyze con estado global TripAdvisor.

Objetivo:

- separar contratos y ejecucion de `scrape` y `analyze`;
- formalizar estado `needs_human`;
- introducir estado global TA con expiracion de cookie;
- habilitar UX de nodos y relanzamiento robusto.

## Historias de usuario

## US26-001 - Estado global TripAdvisor

Como operador, quiero ver y actualizar el estado global de sesion TripAdvisor para saber si puedo lanzar scraping automatico.

Criterios de aceptacion:

- existe documento global en Mongo;
- se almacena `last_human_intervention_at`;
- se calcula y almacena `session_cookie_expires_at`;
- se marca `availability_now` correctamente.

## US26-002 - Scrape job por fuente con nombres distintos

Como operador, quiero lanzar scrape con nombre en Google y nombre en TripAdvisor independientes.

Criterios de aceptacion:

- payload acepta ambos nombres;
- crea jobs por fuente solicitada;
- cada job usa su nombre correspondiente.

## US26-003 - Estado `needs_human` automatico en TripAdvisor

Como operador, quiero que un fallo antibot mueva el job a `needs_human` para intervenir manualmente.

Criterios de aceptacion:

- precheck de sesion invalida -> `needs_human`;
- antibot detectado en runtime -> `needs_human`;
- se guarda diagnostico y error.

## US26-004 - Analisis desacoplado de scraping

Como operador, quiero lanzar analisis sobre scrape jobs ya completados.

Criterios de aceptacion:

- endpoint `POST /analysis/jobs`;
- acepta uno o varios `scrape_job_ids`;
- produce resultado persistido.

## US26-005 - Comentarios trazables por job_id y source

Como operador, quiero que cada comentario quede trazado por job y fuente para auditoria.

Criterios de aceptacion:

- `comments.job_id` obligatorio;
- `comments.source` obligatorio;
- consultas por job/source disponibles.

## US26-006 - Relanzamiento de jobs fallidos o bloqueados

Como operador, quiero relanzar un job desde UI/API sin recrear todo el workflow.

Criterios de aceptacion:

- endpoint de relaunch;
- conserva configuracion original;
- historial de intentos visible.

## Backlog tecnico por tareas

## T26-001 - Crear colecciones y modelos nuevos

Salida:

- `analysis_workflows`, `scrape_jobs`, `analysis_jobs`, `comments`, `tripadvisor_session_state`.

Tests requeridos:

- `UT`: validadores de modelos y enums.
- `IT-DB`: insercion/lectura/indices.
- `E2E`: no aplica.

## T26-002 - Servicio de sesion TripAdvisor

Salida:

- `TripadvisorSessionService` con:
  - refresh desde storage state;
  - parseo de expiracion de cookie;
  - transiciones de estado.

Tests requeridos:

- `UT`: parser de cookies y maquina de estados.
- `IT-DB`: persistencia global state.
- `E2E`: no aplica.

## T26-003 - Integrar precheck de sesion en worker TA

Salida:

- antes de scrape TA, validar estado global;
- setear `needs_human` cuando proceda.

Tests requeridos:

- `UT`: decision precheck.
- `IT-DB`: transicion de status del job.
- `E2E`: scrape TA bloqueado por estado invalido.

## T26-004 - Endpoint `POST /scrape/jobs`

Salida:

- crea workflow + scrape jobs por fuentes.

Tests requeridos:

- `UT`: validacion de payload.
- `IT-DB`: creacion de docs relacionados.
- `E2E`: lanzar job Google-only y both.

## T26-005 - Persistencia de comentarios con `job_id` y `source`

Salida:

- guardar comentarios canonicos en `comments`.

Tests requeridos:

- `UT`: normalizacion de comment.
- `IT-DB`: queries por `job_id` + `source`.
- `E2E`: scrape real produce comments correctos.

## T26-006 - Endpoint `POST /analysis/jobs`

Salida:

- consume `scrape_job_ids`;
- genera resultado de analisis.

Tests requeridos:

- `UT`: validacion de reglas de entrada.
- `IT-DB`: carga comentarios y guarda resultado.
- `E2E`: analisis sobre comments de Google/TA.

## T26-007 - Endpoint/accion manual de confirmacion sesion

Salida:

- confirmar sesion humana;
- actualizar global state;
- relanzar jobs `needs_human`.

Tests requeridos:

- `UT`: reglas de relanzamiento.
- `IT-DB`: update global state + jobs pendientes.
- `E2E`: flujo needs_human -> manual -> completed.

## T26-008 - API de eventos por workflow

Salida:

- SSE `GET /workflows/{id}/events`.

Tests requeridos:

- `UT`: serializacion de eventos.
- `IT-DB`: orden y filtro por workflow.
- `E2E`: stream en vivo durante scrape/analyze.

## T26-009 - UI nodal y panel persiana

Salida:

- canvas + nodos + panel plegable derecha/abajo.

Tests requeridos:

- `UT`: helpers de estado/progreso UI.
- `IT-DB`: no aplica.
- `E2E`: smoke UI + eventos SSE.

## T26-010 - Suite de regression y migracion

Salida:

- plan de migracion gradual;
- tests de compatibilidad con endpoints legacy.

Tests requeridos:

- `UT`: adapters legacy.
- `IT-DB`: dual-write y lectura consistente.
- `E2E`: endpoint antiguo sigue operativo durante transicion.

## Definition of Done de la epica

- Endpoints nuevos `scrape` y `analysis` operativos.
- Estado global TripAdvisor funcionando en produccion local.
- `needs_human` operativo y relanzable.
- Comentarios trazables por `job_id` y `source`.
- UX nodal funcional con panel persiana configurable.
- Matriz de tests completada por tarea.

