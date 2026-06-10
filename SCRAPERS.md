# Scrapers

Este archivo explica solo una cosa: como leer el flujo real de scraping del repo sin perderse.

## Regla principal

Si quieres entender la pipeline de una fuente, no empieces por `browser_scraper.py`.

Empieza por:

- `src/scraping_google_maps/google_maps_business_page_scraper.py`
- `src/scraping_tripadvisor/tripadvisor_business_page_scraper.py`

Esos dos archivos son los que orquestan la secuencia de etapas.

Los archivos `browser_scraper.py` de cada fuente son **composition roots**: agrupan facetas, pero no cuentan la historia completa del flujo.

## Mapa rapido

El camino real es este:

`local worker / caller -> browser adapter -> BusinessService -> business page scraper -> browser scraper -> facet modules`

Mas concreto:

1. Un job browser-driven entra por el runtime local.
2. El adapter de fuente delega en `BusinessService.scrape_business_for_analysis_pipeline(...)`.
3. `BusinessService` termina llamando al scraper de pagina de negocio de la fuente.
4. Ese scraper de pagina de negocio ejecuta la pipeline de alto nivel.
5. El `browser_scraper.py` de la fuente aporta las primitivas de navegador.
6. Las facetas implementan cada paso concreto del navegador.

## Google Maps

### Donde leer la pipeline

Archivo principal:

- `src/scraping_google_maps/google_maps_business_page_scraper.py`

Secuencia principal:

1. Resolver limites efectivos de scraping
2. Emitir `scraper_starting`
3. `self._scraper.start()`
4. `self._scraper.search_business(...)`
5. `self._scraper.extract_listing()`
6. `self._scraper.extract_reviews(...)`
7. `self._scraper.close()` en `finally`

En otras palabras:

`start -> search -> listing -> reviews -> close`

### Donde vive cada cosa

Root del navegador:

- `src/scraping_google_maps/browser_scraper.py`

Facetas:

- `browser_lifecycle_facet.py`
  - abrir/cerrar browser, context, page
- `browser_navigation_facet.py`
  - busqueda y movimiento inicial por Maps
- `browser_listing_facet.py`
  - extraccion de la ficha
- `browser_reviews_open_facet.py`
  - apertura del panel de reseñas
- `browser_reviews_feed_facet.py`
  - control del feed y scroll
- `browser_reviews_collection_facet.py`
  - recogida de reseñas
- `browser_review_card_facet.py`
  - parsing de una review individual
- `browser_parsing_facet.py`
  - helpers de parsing y normalizacion

### Donde NO leer la pipeline

- `src/scraping_google_maps/google_maps_browser_adapter.py`

Ese archivo no explica el flujo del navegador. Solo enruta la llamada al servicio de negocio.

## Tripadvisor

### Donde leer la pipeline

Archivo principal:

- `src/scraping_tripadvisor/tripadvisor_business_page_scraper.py`

Secuencia principal:

1. Validar que la sesion de Tripadvisor esta disponible
2. Construir runtime de pipeline con timeouts y limites
3. Emitir `scraper_starting`
4. Ejecutar etapa `start`
5. Esperar `start_delay` si aplica
6. Ejecutar etapa `search`
7. Ejecutar etapa `listing`
8. Ejecutar etapa `reviews` con limite blando de tiempo
9. Cerrar browser en `finally`
10. Si algo falla, persistir diagnostico y clasificar anti-bot / needs-human

En otras palabras:

`session check -> start -> optional delay -> search -> listing -> reviews -> diagnostics/close`

### Donde vive cada cosa

Root del navegador:

- `src/scraping_tripadvisor/browser_scraper.py`

Facetas del navegador:

- `browser_lifecycle_facet.py`
  - abrir/cerrar browser
- `browser_search_entry_facet.py`
  - escribir query y enviar busqueda
- `browser_search_typeahead_facet.py`
  - intentar abrir coincidencia exacta desde typeahead
- `browser_search_results_facet.py`
  - elegir y abrir el mejor resultado
- `browser_search_matching_facet.py`
  - heuristicas de matching de titulo/href
- `browser_listing_facet.py`
  - extraccion de la ficha
- `browser_reviews_orchestration_facet.py`
  - orquestacion de recoleccion de reseñas
- `browser_reviews_page_collection_facet.py`
  - recoleccion dentro de una pagina de reseñas
- `browser_reviews_navigation_facet.py`
  - pasar a la siguiente pagina de reseñas
- `browser_reviews_pagination_state_facet.py`
  - snapshot del estado de paginacion
- `browser_review_dom_facet.py`
  - extraccion DOM de reseñas
- `browser_review_owner_reply_facet.py`
  - reply del propietario
- `browser_review_identity_facet.py`
  - ids, identidad y parsing basico
- `browser_page_support_facet.py`
  - cookies, consent, prompts y waits
- `browser_text_facet.py`
  - helpers ligeros de texto

### Que archivo concentra el ruido operativo

- `src/scraping_tripadvisor/tripadvisor_business_page_scraper.py`

Ese archivo no solo orquesta la ruta feliz. Tambien gestiona:

- timeout por etapa
- diagnosticos persistidos
- deteccion de anti-bot
- conversion a `ScrapeBotDetectedError`
- conversion a `ScrapeNeedsHumanInterventionError`

Por eso es normal que sea mas denso que Google Maps, aunque ahora la ruta feliz ya se lea bastante mejor.

## Browser adapters

Archivos:

- `src/scraping_google_maps/google_maps_browser_adapter.py`
- `src/scraping_tripadvisor/tripadvisor_browser_adapter.py`

Responsabilidad real:

- no hacen scraping
- no contienen pipeline de navegador
- solo traducen `AnalyzeBusinessTaskPayload` al caso de uso del servicio

Piensalos como puente de runtime, no como corazon del scraper.

## Que leer si quieres cambiar comportamiento

### Cambiar la secuencia de etapas

Toca:

- `google_maps_business_page_scraper.py`
- `tripadvisor_business_page_scraper.py`

### Cambiar como se busca un negocio

Toca:

- Google Maps: `browser_navigation_facet.py`
- Tripadvisor: `browser_search_entry_facet.py`, `browser_search_typeahead_facet.py`, `browser_search_results_facet.py`

### Cambiar como se abre o recorre la zona de reseñas

Toca:

- Google Maps: `browser_reviews_open_facet.py`, `browser_reviews_feed_facet.py`, `browser_reviews_collection_facet.py`
- Tripadvisor: `browser_reviews_orchestration_facet.py`, `browser_reviews_page_collection_facet.py`, `browser_reviews_navigation_facet.py`, `browser_reviews_pagination_state_facet.py`

### Cambiar el parsing de una review

Toca:

- Google Maps: `browser_review_card_facet.py`
- Tripadvisor: `browser_review_dom_facet.py`, `browser_review_owner_reply_facet.py`, `browser_review_identity_facet.py`

## Resumen corto

Si buscas la pipeline, lee los `*business_page_scraper.py`.

Si buscas implementacion de navegador, lee los `browser_scraper.py` y sus facetas.

Si buscas el puente con jobs y runtime, lee los `*browser_adapter.py`.
