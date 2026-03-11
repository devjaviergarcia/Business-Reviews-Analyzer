# Arquitectura del Proyecto

## Estado actual (Fase 0 - baseline real)

El proyecto ya tiene una base funcional de producto y tecnica:

- scraper Google Maps (Playwright)
- pipeline de preprocesado y analisis LLM
- persistencia MongoDB
- API FastAPI con endpoints de analisis y lectura
- jobs asincronos + worker + SSE de progreso

### Fortalezas del estado actual

- Ya existe valor end-to-end (no es una PoC vacia)
- Ya hay datos persistidos y reanalisis
- Ya hay observabilidad minima de progreso (SSE)
- Ya hay scripts de operacion local/docker

### Limites actuales

- Fuerte acoplamiento a Google Maps
- `BusinessService` concentra demasiadas responsabilidades
- Modelo de datos pensado sobre todo para reviews de una fuente
- Arquitectura de workers todavia no separada en scrape/analyze/report

## Arquitectura objetivo (Fases 1+)

### Principio general

Arquitectura hibrida:

- vertical por fuente (cada scraper/conector evoluciona de forma independiente)
- horizontal en el core (normalizacion, matching, analisis, reporting, jobs)

### Workers objetivo

1. `scrape-worker`
   - recolecta datos por fuente
   - ejecuta browser automation o APIs
   - produce payloads crudos/normalizados iniciales

2. `analysis-worker`
   - ejecuta preprocesado
   - resuelve entity matching y deduplicacion
   - ejecuta analisis LLM en modos distintos

3. `report-worker`
   - compone informe estructurado
   - exporta PDF/Typst/HTML
   - genera artefactos compartibles

### Broker y orquestacion recomendados

- RabbitMQ como broker
- workers propios (sin depender de Celery como capa principal)
- colas por etapa + DLQ + reintentos + idempotencia
- estados persistidos en Mongo y emitidos por SSE/WebSocket

## Modelos clave que hay que introducir

- `BusinessIdentity` / `BusinessProfile` (nombre, direccion, ciudad, pais, idioma, aliases)
- `SourceProfile` (identidad del negocio en una fuente concreta)
- `Mention` / `ReviewMention` (modelo canonico de evidencia)
- `AnalysisRun` (tipo de analisis, config, salida, metadatos)
- `ReportRun` (plantilla, formato, version, artefacto generado)

## Scaffolding recomendado para evolucionar sin romper

Evolucion incremental (sin reescribir todo de golpe):

1. Extraer contratos/interfaces (connectors, repositories, analyzers, reporters)
2. Adaptar Google Maps al contrato nuevo
3. Separar workers por responsabilidad
4. Introducir modelo canonico multi-fuente
5. Integrar nuevas fuentes una a una
6. Introducir generacion de informe y UI

## Relacion con el sistema de documentacion contextual

El script `scripts/generate_context_docs.py` mantiene:

- `scaffold_version.json` (estructura versionada del codigo)
- `scaffold_context_input.md` (descripciones editables)
- `scaffold_context.md` (salida legible del scaffold)
- `context_dictionary.md` (indice de markdowns)

Usar estos archivos en cada fase para no perder contexto ni trazabilidad arquitectonica.
