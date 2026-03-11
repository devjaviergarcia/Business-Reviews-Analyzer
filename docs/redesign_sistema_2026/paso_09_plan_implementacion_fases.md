# Paso 09 - Plan de Implementacion por Fases

## Fase 0 - Preparacion

Objetivo:

- congelar contrato actual;
- preparar rama de migracion;
- habilitar feature flags.

Entregables:

- ADR corto de migracion;
- checklist de riesgo;
- baseline de tests actual.

## Fase 1 - Dominio y persistencia

Objetivo:

- introducir nuevas colecciones y modelos.

Entregables:

- `analysis_workflows`, `scrape_jobs`, `analysis_jobs`, `comments`, `tripadvisor_session_state`;
- indices Mongo.

Salida verificable:

- tests unitarios + IT-DB verdes.

## Fase 2 - Estado global TripAdvisor

Objetivo:

- implementar servicio de sesion global.

Entregables:

- parser de `storage_state` para expiracion cookie;
- API de lectura/refresh/confirmacion;
- reglas `valid/invalid/expired`.

Salida verificable:

- tests de integracion sin navegador usando storage states existentes.

## Fase 3 - Endpoints nuevos scrape/analyze

Objetivo:

- separar publicamente ambos flujos.

Entregables:

- `POST /scrape/jobs`;
- `POST /analysis/jobs`;
- `GET /workflows/{id}` + SSE eventos.

Salida verificable:

- e2e API de creacion y seguimiento.

## Fase 4 - Workers y `needs_human`

Objetivo:

- integrar reglas de sesion TA y estado `needs_human`.

Entregables:

- precheck TA en worker;
- transicion a `needs_human`;
- relanzamiento post-intervencion.

Salida verificable:

- e2e de antibot simulado y reanudacion manual.

## Fase 5 - Persistencia canonica de comments

Objetivo:

- guardar comentarios por `job_id` y `source`.

Entregables:

- writer canonico en `comments`;
- query APIs por job/source;
- dual-write temporal con modelo legacy.

Salida verificable:

- integracion DB + e2e scraping.

## Fase 6 - UI Nodal

Objetivo:

- desplegar canvas nodal y panel persiana.

Entregables:

- nodos por estado;
- panel derecha/abajo configurable;
- acciones de relanzar/manual.

Salida verificable:

- smoke UI + feedback del operador.

## Fase 7 - Limpieza y deprecacion

Objetivo:

- retirar rutas/estructuras legacy segun uso real.

Entregables:

- plan de deprecacion formal;
- comunicacion de cambios;
- cleanup tecnico.

Salida verificable:

- no regresiones y metrica estable por 2 ciclos.

## Dependencias criticas

- Fase 2 depende de Fase 1.
- Fase 4 depende de Fase 2 y Fase 3.
- Fase 6 depende de Fase 3 (API workflow/eventos).

## Riesgos y mitigacion

- Riesgo: rotura de flujo actual.
  - Mitigacion: feature flags y dual-write.
- Riesgo: TA inestable por antibot.
  - Mitigacion: `needs_human` como estado operativo, no error terminal.
- Riesgo: sobrecarga de UI.
  - Mitigacion: diseno por capas y pruebas de usabilidad tempranas.

## Gate de finalizacion del programa

- scrape/analyze separados y operativos;
- estado global TA confiable;
- comments trazables por `job_id` + `source`;
- UI nodal usable con acciones de operacion;
- suite de tests definida en Paso 06 ejecutada en CI local.

