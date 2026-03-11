# Fase 1 - Arquitectura modular y workers (diseno + base de implementacion)

## Objetivo

Remodelar la arquitectura actual para prepararla para crecimiento multi-fuente, separando responsabilidades y definiendo claramente los 3 workers:

1. `scrape-worker`
2. `analysis-worker`
3. `report-worker`

Con una orquestacion preparada para RabbitMQ y contratos de mensajes estables.

## Decision de alcance (Marzo 2026)

- `RabbitMQ`: diferido temporalmente.
- `Broker activo en esta fase`: Mongo (`WORKER_BROKER_BACKEND=mongo`).
- `Estado`: se mantiene como pendiente documentado dentro de Fase 1, sin implementacion operativa ahora.

## Problema que resuelve esta fase

Hoy el sistema funciona, pero el crecimiento a nuevas fuentes se vuelve caro porque:

- el servicio de negocio central concentra demasiada logica
- scraping, analisis y persistencia estan demasiado juntos
- no hay un contrato de mensajes entre etapas
- no hay workers especializados por tipo de carga

## Alcance de producto

### Lo que SI cambia de cara a producto

- mejor trazabilidad del progreso del job por etapas
- base para soportar mas fuentes sin rehacer endpoints
- menor riesgo de regresion al crecer

### Lo que NO se promete aun

- nuevas fuentes funcionando en produccion (eso empieza despues)
- UI nueva
- informe PDF final

## Alcance tecnico

### Objetivos tecnicos principales

- separar orquestacion en casos de uso / servicios especializados
- definir contratos de eventos/comandos entre workers
- definir topologia de colas (aunque RabbitMQ se active por pasos)
- mantener compatibilidad con el baseline actual

### Componentes a introducir o refactorizar

- `scrape-worker` (solo recoleccion)
- `analysis-worker` (preprocesado + LLM + agregacion de analisis)
- `report-worker` (generacion de artefactos de informe)
- `job orchestrator` / coordinador de etapas
- `message schema` (comandos/eventos)
- `repositories` / capa de acceso a datos mas modular

## Paso a paso (orden recomendado)

### Paso 1 - Diseñar la pipeline por etapas

Definir flujo canonical del job:

1. `job.created`
2. `scrape.requested`
3. `scrape.completed`
4. `analysis.requested`
5. `analysis.completed`
6. `report.requested`
7. `report.completed`
8. `job.completed` / `job.failed`

Para cada etapa definir:

- input requerido
- output esperado
- errores recuperables/no recuperables
- politicas de retry

### Paso 2 - Definir el contrato de mensajes

Cada mensaje debe incluir como minimo:

- `job_id`
- `business_id` (si existe) o `business_query`
- `stage`
- `attempt`
- `created_at`
- `correlation_id`
- `payload`
- `meta` (source, strategy, mode, etc.)

Regla clave: mensajes pequeños, datos pesados en Mongo (o storage), y el mensaje referencia IDs.

### Paso 3 - Diseñar colas y routing (RabbitMQ target)

Definir topologia objetivo:

- exchange `commands`
- exchange `events`
- queue `scrape.jobs`
- queue `analysis.jobs`
- queue `report.jobs`
- queues DLQ por etapa (opcional desde inicio pero recomendado)

Definir tambien `routing_key` por etapa y, a futuro, por fuente (`scrape.google_maps`, `scrape.tripadvisor`, etc.).

### Paso 4 - Separar el codigo actual por responsabilidad

Extraer desde `BusinessService` en piezas mas pequenas:

- servicio de query/read endpoints
- servicio de jobs/estado
- caso de uso de analisis sincronico
- caso de uso de analisis asincronico (enqueue)
- adaptador del scraper Google Maps
- adaptador del analizador LLM

No hace falta mover todo a DDD formal en esta fase, pero si separar responsabilidades de forma explicita.

### Paso 5 - Introducir interfaz de workers

Definir contratos (aunque la implementacion inicial siga local o con cola actual):

- `ScrapeTaskPayload`
- `AnalysisTaskPayload`
- `ReportTaskPayload`

Y sus resultados:

- `ScrapeResult`
- `AnalysisResult`
- `ReportResult`

Esto reduce acoplamiento y prepara RabbitMQ.

### Paso 6 - Persistencia de estados y eventos

Asegurar que el estado de job no depende del broker:

- Mongo sigue siendo la fuente de verdad del estado del job
- el broker solo mueve trabajo
- SSE/WebSocket leen de Mongo (y/o stream de eventos persistidos)

Definir un estado por etapa:

- `queued`
- `running`
- `retrying`
- `done`
- `failed`
- `partial` (si aplica en multi-fuente)

### Paso 7 - Estrategia de migracion sin romper endpoints

1. Mantener `POST /business/analyze` como fachada.
2. Internamente mover logica a casos de uso nuevos.
3. Mantener `POST /business/analyze/queue` y SSE.
4. Cambiar la implementacion interna, no la API primero.

### Paso 8 - Introducir RabbitMQ incrementalmente

No migrar todo de golpe.

Propuesta:

- Iteracion A: contratos + workers + cola actual (Mongo polling)
- Iteracion B: producer RabbitMQ para una etapa
- Iteracion C: consumers RabbitMQ para scrape/analysis
- Iteracion D: report-worker y eventos completos

Nota de esta iteracion: este paso queda aplazado; solo se conserva el diseno y backlog de migracion.

### Paso 9 - Validacion tecnica de la fase

Validar:

- compatibilidad de endpoints actuales
- jobs siguen reportando progreso
- analisis end-to-end sigue funcionando con Google Maps
- separacion de responsabilidades mejora mantenibilidad

## Entregables

- Arquitectura de workers definida y documentada
- Contratos de mensajes versionados
- Refactor parcial de servicio monolitico a piezas modulares
- Base para RabbitMQ (aunque la migracion completa se haga por iteraciones)

## Riesgos y mitigaciones

### Riesgo 1 - Sobrerrefactor

Mitigacion: preservar el baseline y mover por capas, no reescribir completo.

### Riesgo 2 - Complejidad de colas demasiado pronto

Mitigacion: introducir RabbitMQ en fases internas, manteniendo API estable.

### Riesgo 3 - Duplicacion de logica temporal

Mitigacion: centralizar contratos y adaptadores, aceptar duplicacion minima temporal con fecha de retirada.

## Criterios de salida

- Existen 3 responsabilidades de worker definidas y trazables.
- Hay contratos de mensajes claros entre etapas.
- La API actual sigue funcionando sobre la nueva estructura.
- El sistema esta listo para introducir modelo canonico multi-fuente (Fase 2).

## Dependencia con la siguiente fase

La Fase 2 utiliza esta separacion para introducir modelo de negocio/fuentes sin volver a mezclar scraping y analisis en un solo servicio.
