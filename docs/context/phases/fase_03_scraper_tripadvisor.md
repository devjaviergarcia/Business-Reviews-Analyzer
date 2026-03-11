# Fase 3 - Integracion scraper de Tripadvisor

## Objetivo

Integrar Tripadvisor como nueva fuente de reseñas/opiniones, utilizando el modelo canonico definido en Fase 2.

Tripadvisor es una fase propia porque su estructura, paginacion, tipos de negocio y restricciones de scraping son muy distintas a Google Maps.

## Valor de negocio de esta fase

- aporta una fuente especialmente relevante para turismo, hoteles, restaurantes y actividades
- aumenta la cobertura y la calidad comparativa del analisis
- permite contrastar percepcion de clientes viajeros vs clientes locales

## Alcance de producto

### Lo que SI se entrega

- descubrimiento y seleccion de ficha de negocio en Tripadvisor
- extraccion de datos de la ficha (source profile)
- extraccion de reviews (texto, rating, fecha, idioma si disponible)
- normalizacion al modelo canonico
- almacenamiento y uso en el pipeline de analisis

### Lo que NO se entrega aun

- analisis avanzado multi-fuente definitivo (se consolida mas adelante)
- UI final
- informe PDF final

## Alcance tecnico

### Retos especificos de Tripadvisor

- multiples verticales (hotel, restaurante, atraccion)
- DOM y paginacion distintos segun tipo de pagina
- reviews con traducciones y bloques expandidos
- potenciales medidas anti-bot
- variaciones por idioma/region

### Decisiones clave

- crear conector vertical `sources/tripadvisor/`
- separar claramente:
  - `search/listing discovery`
  - `profile parser`
  - `reviews parser`
  - `normalizer`
- definir estrategia de retries y ritmo de scraping especifica para esta fuente

## Paso a paso (orden recomendado)

### Paso 1 - Investigacion estructural de la fuente

1. Analizar varios tipos de paginas reales:
   - restaurante
   - hotel
   - atraccion/actividad
2. Documentar selectores y patrones robustos (no IDs volatiles).
3. Identificar como cambia la estructura con idioma distinto.
4. Guardar snapshots HTML/fixtures para pruebas de parser.

### Paso 2 - Definir contrato del conector Tripadvisor

Diseñar interfaz del conector alineada al core:

- `search_business()` / `resolve_profile()`
- `fetch_source_profile()`
- `fetch_reviews()`
- `normalize_reviews_to_mentions()`

Definir que devuelve cada metodo y que errores levanta.

### Paso 3 - Resolver descubrimiento de negocio

1. Implementar busqueda por nombre + ciudad/pais (si se proveen).
2. Aplicar matching contra `BusinessProfile` (Fase 2).
3. Guardar varios candidatos si hay ambiguedad.
4. Elegir candidato con mayor score y registrar evidencia.

### Paso 4 - Extraer `SourceProfile` de Tripadvisor

Extraer campos minimos:

- nombre visible
- url
- ubicacion/direccion (si aplica)
- rating global
- total de reviews
- categoria/tipo de negocio
- metadata de la fuente (por ejemplo vertical detectado)

Guardar como `SourceProfile` con `match_confidence`.

### Paso 5 - Extraer reseñas

1. Abrir panel/listado de reviews.
2. Gestionar paginacion o lazy loading.
3. Expandir textos truncados (si existen).
4. Capturar fecha, rating, idioma, texto, autor, y enlaces relevantes.
5. Capturar payload crudo (o referencia a raw payload).

### Paso 6 - Normalizacion al modelo canonico

Mapear reviews Tripadvisor a `ReviewMention`:

- `source = tripadvisor`
- `source_item_id`
- `source_profile_id`
- `published_at`
- `rating`
- `text`
- `language`
- `url`
- `raw_payload_ref`

Aplicar dedupe intra-fuente por fingerprint.

### Paso 7 - Integracion con pipeline de jobs

1. Añadir `tripadvisor` al routing del `scrape-worker`.
2. Permitir activar/desactivar fuente por payload/config.
3. Emitir progreso por etapas (discovery/profile/reviews/normalize).
4. Persistir errores trazables por fuente sin romper el job entero.

### Paso 8 - Validacion (smoke + e2e)

Casos minimos:

- negocio con match claro
- negocio con multiples candidatos
- negocio sin resultados
- reviews con texto truncado
- pagina con cambios menores de DOM (fallo controlado)

## Entregables

- Conector Tripadvisor operativo
- Parser y normalizador con fixtures
- `SourceProfile` + `ReviewMention` persistidos
- Integracion con workers y eventos de progreso
- Smoke tests de Tripadvisor

## Riesgos y mitigaciones

- **DOM cambiante**: usar selectores por patron/semantica + fixtures.
- **anti-bot**: pacing, retries, perfiles controlados, limites de concurrencia.
- **ambiguedad de negocio**: matching con datos enriquecidos (ciudad/pais/website).

## Criterios de salida

- Se puede recolectar Tripadvisor de forma repetible para al menos 2 tipos de negocio.
- Los datos se almacenan en el modelo canonico sin crear un silo nuevo.
- El pipeline general no se rompe si Tripadvisor falla.

## Dependencia con la siguiente fase

La Fase 4 reutiliza la misma estructura de conector y de normalizacion, pero adaptada a Trustpilot.
