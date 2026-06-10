# Repiq

Repiq es una plataforma local-first de inteligencia reputacional para negocios locales.

El sistema hace cuatro cosas principales:

1. Raspa fuentes públicas como Google Maps y Tripadvisor.
2. Normaliza, persiste y analiza reseñas y fichas de negocio.
3. Genera informes accionables para leads, clientes y estudios públicos.
4. Orquesta discovery, geogrids, benchmark y outreach CRM sobre Mongo y jobs asíncronos.

## Documento canónico

Este `README.md` es la única descripción canónica de la arquitectura del proyecto.

Si otro Markdown del repositorio contradice este archivo, manda este archivo.

`docs/` contiene material auxiliar e histórico:

- backlog y planificación
- contexto antiguo
- editorial y SEO
- checklists operativas
- manifiestos de producto

Ese material puede seguir siendo útil, pero no es la fuente de verdad de la arquitectura actual.

`SCRAPERS.md` complementa este documento con una lectura operativa específica de las pipelines de scraping browser-driven.

## Qué problema resuelve el proyecto

Repiq no es solo un scraper de reseñas.

Es un sistema completo para:

- analizar cómo se percibe un negocio en Google Maps y Tripadvisor
- comparar su desempeño con competidores
- producir informes de reputación listos para enviar
- descubrir leads desde búsquedas locales
- construir estudios geográficos con geogrid
- soportar un funnel CRM manual o semiautomático

## Antes vs ahora

### Antes

La base del proyecto estaba mucho más concentrada en pocos módulos grandes:

- `src/scraper/`
- `src/pipeline/`
- `src/services/`
- routers FastAPI bastante acoplados a servicios grandes
- documentación de arquitectura desactualizada y centrada en una fase previa

El eje real era:

`Google Maps -> Mongo -> preprocess -> LLM -> FastAPI`

Eso funcionaba, pero tenía varios problemas:

- demasiada responsabilidad dentro de `BusinessService`
- demasiada responsabilidad dentro de `CRMService`
- scraping y runtime de navegador mezclados con lógica de negocio
- nombres demasiado genéricos para un sistema ya multi-flujo
- documentación ya no alineada con el código real

### Ahora

La arquitectura ya está bastante más orientada por contexto:

- `src/platform/` define el composition root real
- `src/browser_runtime/` ejecuta Playwright local de forma explícita
- `src/job_runtime/` define contratos y coordinación de jobs browser-driven
- `src/scraping_google_maps/` contiene el scraper real de Google Maps
- `src/scraping_tripadvisor/` contiene el scraper real de Tripadvisor
- `src/business_catalog/` concentra el pipeline de scraping persistido del catálogo de negocios
- `src/crm/` ya está separado por subcontextos
- `src/pipeline/report_rendering/` separa mejor la construcción y render del reporte

Además, el patrón operativo importante ya quedó fijado:

- API y Mongo pueden vivir en Docker o local
- el navegador real vive en tu máquina
- los jobs browser-driven pueden ir en modo `automatic` o `live`
- el runtime local reclama esos jobs y ejecuta Playwright fuera del contenedor

## Mapa de lectura recomendado

Si alguien entra nuevo al repo, este es el orden correcto para entenderlo:

1. `src/main.py`
2. `src/platform/application_root.py`
3. `src/dependencies.py`
4. `src/routers/`
5. `src/job_runtime/`
6. `src/browser_runtime/`
7. `src/business_catalog/`
8. `src/crm/`
9. `src/scraping_google_maps/` y `src/scraping_tripadvisor/`
10. `src/pipeline/` y `src/pipeline/report_rendering/`
11. `scripts/` como entrypoints operativos

## Distribución actual del código

### `src/main.py`

Punto de entrada FastAPI.

Responsabilidad:

- montar la app
- abrir y cerrar Mongo
- registrar routers

No debería contener lógica de negocio.

### `src/platform/`

#### `src/platform/application_root.py`

Es el composition root real del proyecto.

Responsabilidad:

- instanciar servicios, repositorios, runtimes y adapters
- cablear casos de uso
- exponer la composición final que consume FastAPI y los workers

Este archivo es la pieza más importante para entender cómo está montado el sistema hoy.

### `src/dependencies.py`

Es un puente fino entre FastAPI y `ApplicationRoot`.

Responsabilidad:

- resolver dependencias para routers y algunos workers
- exponer funciones `create_*` que realmente devuelven objetos ya cableados por el root

Estado actual:

- sí se usa
- es explícito y legible
- sigue siendo una capa puente, no dominio

### `src/job_runtime/`

Contexto de contratos y coordinación de jobs browser-driven.

Archivos clave:

- `browser_job_contracts.py`
- `local_browser_job_coordinator.py`

Responsabilidad:

- definir `source`, `execution_mode`, `runtime_target`, `fallback_policy`
- decidir qué jobs son del runtime local
- reclamar jobs pendientes para el worker local

Esto es la base del patrón local-first actual.

### `src/browser_runtime/`

Runtime local de navegador.

Archivos clave:

- `local_browser_runtime_worker.py`
- `local_browser_worker_registry.py`

Responsabilidad:

- hacer heartbeat del worker local
- reclamar jobs browser-driven
- ejecutar Google Maps o Tripadvisor localmente
- soportar `automatic` y `live`
- devolver progreso y estado a Mongo

Este módulo es la pieza que permite que Playwright corra en tu máquina en vez de en Docker.

### `src/scraping_google_maps/`

Implementación real del scraping de Google Maps.

Archivos clave:

- `browser_scraper.py`
- `browser_scraper_facets/*`
- `google_maps_business_page_scraper.py`
- `google_maps_browser_adapter.py`
- `selectors.py`

Responsabilidad:

- controlar Playwright para Google Maps
- abrir ficha, capturar listing y reseñas
- adaptarse al contrato browser-driven del runtime local

Estado:

- `browser_scraper.py` ya no es el monolito principal
- ahora actúa como composition root del scraper
- la lógica se reparte por facetas de ciclo de vida, navegación, listing, reviews y parsing

### `src/scraping_tripadvisor/`

Implementación real del scraping de Tripadvisor.

Archivos clave:

- `browser_scraper.py`
- `browser_scraper_facets/*`
- `browser_scraper_types.py`
- `tripadvisor_business_page_scraper.py`
- `tripadvisor_browser_adapter.py`
- `tripadvisor_scrape_diagnostics.py`

Responsabilidad:

- scraping browser-driven de Tripadvisor
- diagnóstico de antibot y sesiones humanas
- adaptación al runtime local

Estado:

- `browser_scraper.py` ya no concentra la lógica principal
- ahora actúa como composition root del scraper
- la lógica quedó repartida por subcontextos explícitos:
  - entrada y envío de búsqueda
  - matching, typeahead y resultados
  - apertura de listing
  - orquestación y colección de reseñas
  - navegación y estado de paginación
  - extracción DOM
  - reply del propietario
  - identidad y parsing de review
- hay facetas-facade pequeñas que solo agrupan otras facetas más concretas
- el patrón interno ya es comparable al de Google Maps:
  - root pequeño
  - facetas agrupadas por contexto real
  - helpers compartidos mínimos

### `src/scraping_shared/`

Código realmente compartido entre scrapers browser-driven.

Archivos clave:

- `browser_scrape_adapter.py`
- `browser_scrape_errors.py`

Responsabilidad:

- contrato común de adapter
- taxonomía común de errores de scraping

### `src/scraper/`

Compatibilidad, no implementación principal.

Estado real:

- `src/scraper/google_maps.py` y `src/scraper/tripadvisor.py` son wrappers
- reexportan las implementaciones nuevas de `src/scraping_google_maps/` y `src/scraping_tripadvisor/`
- todavía se usan en scripts y algún test histórico

Conclusión:

- no es código muerto todavía
- tampoco es ya la fuente principal del scraping
- es una capa de compatibilidad que se puede retirar cuando scripts y tests migren del todo

### `src/business_catalog/`

Contexto del catálogo de negocio y pipeline de scraping persistido.

Archivos clave:

- `business_scrape_pipeline_runner.py`
- `business_scrape_run_store.py`
- `business_source_persistence.py`
- `enqueue_browser_scrape_jobs_use_case.py`
- `relaunch_browser_scrape_job_use_case.py`

Responsabilidad:

- orquestar scraping por fuente
- persistir perfiles por fuente, reseñas y datasets
- crear snapshots de scraping
- reencolar scrapes y relanzamientos

Este contexto es el puente entre scraping puro y el resto del producto.

### `src/services/business_service.py`

Fachada principal del dominio de análisis de negocio.

Responsabilidad actual:

- fachada principal del dominio de análisis de negocio
- orquestación del scrape, preprocess, analysis y reporting
- coordinación con `business_catalog`
- persistencia y relanzamientos

Estado:

- sí se usa intensamente
- ya no concentra toda la implementación en un solo archivo
- ahora funciona como composition root con facetas semánticas
- sigue siendo una fachada importante, pero ya no es uno de los peores cachalotes del repo

### `src/services/analysis_job_service.py`

Contexto de jobs de análisis.

Responsabilidad:

- encolar jobs
- marcar estados
- persistir eventos y progreso
- servir de backbone operativo para workers

Sí es parte central del runtime actual.

### `src/services/business_query_service.py`

Contexto de lectura y consulta.

Responsabilidad:

- leer negocios, reseñas, análisis, jobs y artefactos
- responder a endpoints de consulta

### `src/crm/`

Ahora ya está separado por subcontextos semánticos.

#### `src/crm/leads/`

Responsabilidad:

- CRUD de leads
- cola de lead discovery y pipeline
- sincronización de referencias del funnel
- procesamiento de tareas asociadas a leads

#### `src/crm/report_requests/`

Responsabilidad:

- solicitudes de informe
- feedback sobre informes
- listado, retry y procesado pendiente

#### `src/crm/studies/`

Responsabilidad:

- benchmark studies
- geogrid studies
- generación de lead report, paid report y public study
- listados y estadísticas de discovery y geogrid

#### `src/crm/campaigns/`

Responsabilidad:

- campañas CRM
- dispatch jobs
- mensajes y eventos
- webhook de Resend

#### `src/crm/discovery/`

Responsabilidad:

- discovery live en Google Maps
- orquestación de discovery
- lectura persistida de discovery

#### `src/crm/repositories/`

Responsabilidad:

- contratos de persistencia CRM
- implementación Mongo

#### `src/crm/benchmark/`

Responsabilidad:

- benchmark competitivo
- geo points
- UULE
- geogrid data model
- selección de competidores

### `src/services/crm_service.py`

Fachada principal del contexto CRM.

Estado real:

- sí se usa en producción local del proyecto
- ya delega a casos de uso, runtimes y facetas contextuales
- el archivo principal ahora es sobre todo composition root y wiring visible
- aún conserva piezas legacy y de transición, pero ya no es un monolito de 1.700+ líneas

Conclusión:

- ha mejorado mucho estructuralmente
- sigue siendo una fachada relevante, pero ahora es bastante más rastreable

### `src/pipeline/`

Contexto del análisis y ensamblado de payloads de report.

Archivos clave:

- `preprocessor.py`
- `llm_analyzer.py`
- `advanced_report_builder.py`
- `advanced_report_action_plan_builder.py`
- `advanced_report_annex_builder.py`
- `advanced_report_payload_assembly.py`
- `advanced_report_preview_builder.py`
- `advanced_report_source_insights_builder.py`
- `report_renderer.py`

Responsabilidad:

- limpiar y preparar reseñas
- ejecutar análisis LLM
- construir el payload estructurado del informe
- renderizar artefactos finales

### `src/pipeline/report_rendering/`

Contexto de render final del informe.

Archivos clave:

- `final_report_document.py`
- `preview_report_document.py`
- `annex_report_document.py`
- `artifact_layout.py`
- `pdf_export.py`
- `font_embedding.py`
- `charts.py`
- `sections.py`
- `generators/section_generators.py`

Responsabilidad:

- convertir payload estructurado en HTML, PDF, anexos y CSV
- separar layout, secciones, charts y export

Aquí el refactor ya dejó una estructura bastante más legible que antes.

### `src/routers/`

Frontera HTTP.

Archivos clave:

- `business.py`
- `crm.py`
- `analysis.py`
- `tripadvisor.py`
- `browser_runtime.py`
- `editorial.py`
- `health.py`

Responsabilidad:

- validar request/response
- invocar casos de uso o servicios compuestos
- mapear errores a HTTP

### `src/workers/`

Infraestructura clásica de workers asíncronos.

Archivos clave:

- `analysis_worker.py`
- `report_worker.py`
- `crm_worker.py`
- `scraper_worker.py`
- `mongo_broker.py`
- `contracts.py`
- `events.py`

Responsabilidad:

- ejecutar trabajos de cola tradicionales
- manejar jobs que no pasan por el runtime local o que conviven con él

Estado:

- sigue siendo parte importante del sistema
- convive con el runtime local nuevo

### `apps/manager/`

Interfaz local de operación.

Responsabilidad:

- supervisar jobs
- consultar CRM
- lanzar pipelines y estudios
- servir como panel operativo del sistema

Importante:

- `apps/manager/dist/` y `apps/manager/node_modules/` son output y dependencias locales
- no forman parte de la arquitectura de dominio

### `scripts/`

Tooling operativo y utilidades locales.

Responsabilidad:

- lanzar workers
- hacer smoke tests
- automatizar tareas de soporte
- capturar debugging puntual

No deben leerse como dominio. Son entrypoints de operación y laboratorio.

## Flujos principales

### 1. Flujo de scraping y análisis de negocio

Entrada típica:

- router `POST /business/*`
- manager local
- scripts operativos

Flujo:

1. se crea o relanza un job de scraping
2. el job queda persistido en Mongo
3. si es browser-driven y `runtime_target=local_browser`, lo reclama `LocalBrowserRuntimeWorker`
4. el adapter de Google Maps o Tripadvisor ejecuta Playwright local
5. `business_catalog` persiste listing, reseñas, dataset y scrape run
6. el pipeline analiza contenido con preprocess + LLM
7. `report_renderer` genera artefactos del informe

### 2. Flujo `automatic` vs `live`

#### `automatic`

- modo por defecto
- ejecución silenciosa del scraping
- pensado para throughput y reintento

#### `live`

- activación explícita
- navegador visible
- pensado para antibot, validación manual o intervención humana

La decisión de modo viaja en `job_runtime/browser_job_contracts.py`.

### 3. Flujo de lead discovery CRM

Entrada típica:

- endpoint CRM
- manager local

Flujo:

1. se crea discovery run
2. se encola un job CRM de discovery
3. si requiere navegador real, lo reclama el runtime local
4. discovery live en Google Maps extrae candidatos
5. se persisten leads, pasos y eventos
6. el pipeline CRM puede seguir con benchmark o campañas

### 4. Flujo de report request

1. entra una solicitud de informe
2. se registra el request y su contexto
3. se lanza benchmark/geogrid según el caso
4. se genera lead report, paid report o public study
5. se guarda feedback posterior si aplica

### 5. Flujo de geogrid

1. se crea un geo grid run
2. se resuelven ciudad, grid y provider mode
3. se lanza estudio
4. se calculan puntos, ranking y stats
5. el resultado se integra en reporting o estudio público

### 6. Flujo de campañas CRM

1. se crea campaña
2. se seleccionan leads elegibles
3. se encolan dispatch jobs
4. se envían mensajes o emails
5. se reciben eventos y webhooks de Resend
6. se persisten mensajes, eventos y estado de campaña

## Qué está realmente en uso

### Canónico y activo

Estas piezas son hoy la fuente real de verdad del sistema:

- `src/platform/application_root.py`
- `src/dependencies.py`
- `src/main.py`
- `src/browser_runtime/*`
- `src/job_runtime/*`
- `src/scraping_google_maps/*`
- `src/scraping_tripadvisor/*`
- `src/scraping_shared/*`
- `src/business_catalog/*`
- `src/crm/*`
- `src/pipeline/*`
- `src/pipeline/report_rendering/*`
- `src/services/analysis_job_service.py`
- `src/services/business_service.py`
- `src/services/crm_service.py`
- `src/services/business_query_service.py`
- `src/routers/*`
- `src/workers/*` salvo el broker RabbitMQ placeholder

### Compatibilidad todavía viva

Estas piezas no son la arquitectura objetivo, pero todavía se usan:

- `src/scraper/*`
  - wrappers para scripts y tests históricos
- `src/business_catalog/legacy_review_dataset_packager.py`
  - puente para empaquetado dataset legacy
- `src/crm/*/legacy_*.py`
  - runtimes heredados aún llamados desde `CRMService`
- `src/services/tripadvisor_local_worker_control_service.py`
  - puente con el bridge local antiguo si está activado por settings
- `scripts/tripadvisor_local_worker_bridge.py`
  - compatibilidad operativa, no pieza arquitectónica central

### Placeholder o no implementado de verdad

- `src/workers/rabbitmq_broker.py`
  - existe como placeholder
  - no es el backend real del sistema hoy
  - la operación actual sigue sobre Mongo

## Qué sigue siendo un cachalote

La estructura general ya mejoró, pero estos archivos siguen siendo demasiado grandes y son los siguientes candidatos claros a seguir partiendo:

- `src/pipeline/advanced_report_builder.py`
- `src/services/analysis_job_service.py`
- `src/crm/repositories/mongo.py`
- `src/services/business_query_service.py`
- `src/business_catalog/business_scrape_pipeline_runner.py`
- `src/crm/discovery/google_maps_live_discovery_runtime.py`
- `src/platform/application_root.py`
- `src/workers/scraper_worker.py`
- `src/workers/contracts.py`
- `src/routers/business.py`
- `src/routers/crm.py`
- `src/scraping_tripadvisor/tripadvisor_scrape_diagnostics.py`

Conclusión honesta:

- los “archivos gigantes” ya no están tan mezclados como antes
- varios de los antiguos monolitos ya se convirtieron en roots + facetas
- los scrapers browser-driven principales ya quedaron divididos por fuente y por subcontextos internos
- el siguiente frente grande ya no está en scraping principal, sino en reporting, queries, repositorios, runtimes y routers

## Qué es ruido operativo y no debe leerse como parte del diseño

Estas rutas o directorios no son diseño del sistema, sino runtime local, caché o output:

- `artifacts/`
- `playwright-data*/`
- `.venv-uv/`
- `__pycache__/`
- `.pytest_cache/`
- `apps/manager/node_modules/`
- `apps/manager/dist/`

Importante:

- `playwright-data*/` puede contener perfiles, sesiones y estado útil de operación
- `artifacts/` puede contener informes, logs y salidas útiles
- por eso no conviene borrarlos a ciegas aunque estén fuera del diseño del repo

## Qué hay en `docs/`

`docs/` no es arquitectura viva. Es material auxiliar.

### `docs/backlogs/`

Planificación, épicas y tickets.

### `docs/context/`

Contexto antiguo del proyecto. Útil para historia, no para leer la arquitectura actual.

### `docs/editorial/`

Piezas de contenido y publicación.

### `docs/ops/`

Checklist manual de operación.

### `docs/process/`

Notas de proceso interno.

### `docs/product/`

Notas de producto específicas.

### `docs/seo/`

Notas SEO y material de investigación.

## Estado de limpieza del repo

### Mejoras ya visibles

- CRM ya no vive como un paquete plano de casos de uso sueltos
- scrapers reales ya están separados por fuente
- el scraper de Google Maps ya está partido por facetas internas
- el scraper de Tripadvisor ya quedó partido en roots pequeños + facetas semánticas
- el runtime local de navegador ya existe como contexto explícito
- el rendering del reporte ya está más partido que antes
- `BusinessService` y `CRMService` ya actúan como composition roots visibles y rastreables

### Transición todavía abierta

- `BusinessService` y `CRMService` siguen siendo fachadas importantes
- el stack aún convive con compatibilidad legacy
- Tripadvisor ya no está concentrado en un solo archivo; lo que queda más pesado ahí es diagnóstico y soporte periférico
- los routers todavía son bastante voluminosos
- hay scripts históricos que aún apuntan a wrappers antiguos

## Política práctica para mantener el repo entendible

1. Si algo es dominio o runtime real, debe vivir en `src/` con nombre contextual.
2. Si algo solo sirve para compatibilidad, debe decir `legacy` o quedar claro como wrapper.
3. Si algo es operativo o debugging, debe ir en `scripts/`.
4. Si algo es output o runtime local, debe quedarse fuera del diseño y fuera del control mental del lector.
5. Si aparece una contradicción entre varios documentos, este `README.md` es el mapa válido.

## Entry points operativos principales

### API

```bash
uvicorn src.main:app --reload
```

### Worker local de navegador

```bash
python3 scripts/run_local_browser_runtime_worker.py
```

### Workers de cola clásicos

Se lanzan desde sus scripts o desde tu flujo local habitual según la etapa que quieras ejecutar.

## Resumen corto

La foto honesta del proyecto hoy es esta:

- ya no es un scraper monolítico con un par de services gigantes y poco más
- ya existe una arquitectura bastante más contextual
- el runtime local de navegador es una mejora estructural importante y real
- CRM, scraping y reporting están bastante mejor separados que antes
- Google Maps ya quedó razonablemente partido por responsabilidades internas
- Tripadvisor ya quedó con la misma idea de root + facetas contextuales
- todavía quedan cachalotes y capas legacy, pero ahora se ven claramente y no están escondidos

Ese cambio importa porque hace que el sistema ya sea mucho más legible, más operable y más fácil de seguir refactorizando sin romper el producto.
