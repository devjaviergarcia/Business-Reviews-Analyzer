# Fase 4 - Integracion scraper de Trustpilot

## Objetivo

Integrar Trustpilot como fuente estructurada de reseñas, normalmente orientada a empresas/servicios, utilizando el modelo canonico multi-fuente.

## Valor de negocio de esta fase

- aporta una señal fuerte para negocios con presencia digital/servicios
- reviews frecuentemente mas extensas y criticas
- permite comparar experiencia local (Google Maps) vs experiencia de servicio/compra (Trustpilot)

## Alcance de producto

### Lo que SI se entrega

- resolucion de perfil de empresa en Trustpilot
- extraccion de ficha y reseñas
- normalizacion y almacenamiento canonico
- participacion en pipeline de analisis

### Lo que NO se entrega aun

- consolidacion final avanzada de insights multi-fuente (eso se refina en fases de analisis)
- informe visual final

## Alcance tecnico

### Caracteristicas utiles de Trustpilot

- estructura de reseñas relativamente consistente
- rating y metadatos claros
- company profile util para source truth secundaria

### Retos

- cambios de layout/experimentos A/B
- paginacion y filtros
- politicas de acceso/rate limits
- localizacion de idiomas/mercados

## Paso a paso (orden recomendado)

### Paso 1 - Analisis de estructura y fixtures

1. Capturar ejemplos de paginas de empresas de distintos paises.
2. Identificar patrones estables para:
   - nombre de empresa
   - rating global
   - numero de reseñas
   - bloque de review
3. Preparar fixtures HTML para parser offline.

### Paso 2 - Definir conector Trustpilot

Implementar `sources/trustpilot/` con piezas separadas:

- `connector`
- `profile_parser`
- `reviews_parser`
- `normalizer`
- `matching_adapter`

Mantener interfaz comun con otras fuentes.

### Paso 3 - Descubrimiento y matching del negocio

1. Buscar por nombre y pais/idioma cuando exista.
2. Generar candidatos.
3. Aplicar score de matching con `BusinessProfile`.
4. Registrar evidencia de por que se selecciona una empresa.

### Paso 4 - Extraccion de `SourceProfile`

Capturar al menos:

- nombre
- url
- rating global
- total de reseñas
- categoria (si existe)
- pais/region (si disponible)
- descripcion breve / metadata (si existe)

### Paso 5 - Extraccion de reseñas

1. Iterar paginas o bloques de reviews.
2. Capturar rating, texto, fecha, autor, idioma/mercado (si visible), y metadatos.
3. Detectar respuestas de empresa (si existen) y mapearlas al modelo canonico.
4. Persistir raw payload y fingerprint.

### Paso 6 - Normalizacion y dedupe

Mapear a `ReviewMention` y aplicar:

- dedupe intra-fuente
- control de idioma
- parseo de fechas consistente
- sanitizacion de texto

### Paso 7 - Integracion en `scrape-worker`

1. Routing por fuente `trustpilot`.
2. Eventos de progreso:
   - discovery
   - source_profile
   - review_pages
   - normalize
3. Manejo de fallos parciales sin abortar todo el job si otras fuentes siguen vivas.

### Paso 8 - Validacion y calidad de datos

Validar:

- empresa correcta vinculada al negocio objetivo
- numero de reseñas razonable
- ratings bien parseados
- respuestas de empresa bien capturadas (si aplican)
- trazabilidad de errores

## Entregables

- Conector Trustpilot operativo
- Parser/normalizador con fixtures
- Integracion con `scrape-worker`
- Persistencia canonica (`SourceProfile`, `ReviewMention`)
- Smoke tests de Trustpilot

## Riesgos y mitigaciones

- **Matching incorrecto por nombres similares**: usar pais, website, aliases.
- **Layout variable**: parser por patrones y tests con fixtures.
- **Bloqueos o limitaciones**: throttling y politicas de reintento.

## Criterios de salida

- Trustpilot puede aportar datos normalizados para analisis de un negocio.
- El conector respeta el contrato comun de fuentes.
- La calidad de datos es suficiente para comparativa con Google Maps.

## Dependencia con la siguiente fase

La Fase 5 (Reddit) introduce datos menos estructurados, por lo que se apoya en este trabajo de modelo canonico y matching pero con mayor enfasis en menciones sin rating.
