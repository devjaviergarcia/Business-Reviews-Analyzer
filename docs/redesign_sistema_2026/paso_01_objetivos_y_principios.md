# Paso 01 - Objetivos y Principios

## Objetivo principal

Redisenar el sistema para que el flujo quede claramente separado en dos etapas:

1. `scrape` (obtencion de comentarios por fuente).
2. `analyze` (analisis semantico sobre comentarios ya obtenidos).

## Objetivos funcionales

- Introducir estado global de sesion TripAdvisor en base de datos.
- Gestionar estado `needs_human` para TripAdvisor de forma nativa.
- Permitir nombres distintos por fuente para un mismo negocio.
- Permitir `scrape` por `google_maps`, `tripadvisor` o ambos.
- Asociar cada comentario a `job_id` y `source`.
- Mantener trazabilidad completa de eventos/errores por job.

## Objetivos no funcionales

- Idempotencia de jobs.
- Observabilidad total (eventos, transiciones, timestamps).
- Recuperacion operativa sencilla (reanudar, relanzar, intervention manual).
- Compatibilidad con la base actual de workers y Mongo.

## Principios de diseno

- Separacion de responsabilidades:
  - Scrapers no hacen analisis.
  - Analizador no scrapea.
- Modelo orientado a jobs:
  - estado y progreso son datos de primer nivel.
- Estado global explicito para dependencias externas:
  - TripAdvisor session state no debe quedar implito en logs.
- Reintentos controlados:
  - fallos recuperables no rompen toda la pipeline.
- Evolucion incremental:
  - se migra por fases sin romper endpoints operativos existentes.

## Problemas actuales que se corrigen

- Acoplamiento fuerte scrape + persistencia + handoff.
- Flujo de analisis no siempre alineado con ambas fuentes.
- Falta de un modelo de sesion global de TripAdvisor.
- Falta de estado `needs_human` como estado formal de dominio.

