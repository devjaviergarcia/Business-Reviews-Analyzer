# Fase 2 - Modelo canonico de negocio y fuentes (source truth + centralizacion)

## Objetivo

Crear el modelo de datos base para operar con multiples fuentes reales y centralizar todo alrededor de un negocio.

Esta fase es clave porque define la estructura que permitira conectar Tripadvisor, Trustpilot, Reddit y futuras fuentes sin rehacer el core cada vez.

## Problema que resuelve

Hoy el sistema esta muy orientado a Google Maps y a reviews de una sola fuente. Para crecer necesitamos:

- una identidad de negocio mas rica que un nombre de busqueda
- una estructura por fuente (source profiles)
- una estructura canonica de evidencia (reviews/menciones)
- una forma de unir todo en un mismo negocio con confianza

## Requisito especial (muy importante en esta fase)

La entrada del analisis debe poder aceptar mas datos desde el inicio:

- nombre del negocio
- direccion
- ciudad
- pais
- idioma principal
- aliases opcionales (nombres alternativos)
- website (si existe)
- telefono (si existe)

Esto no es solo UX: mejora matching entre fuentes y reduce falsos positivos.

## Alcance de producto

### Lo que SI se entrega

- nuevo modelo de entrada para identificar mejor el negocio
- estructura interna para consolidar datos multi-fuente en un negocio
- base para comparativas entre fuentes (aunque aun no esten todas integradas)

### Lo que NO se entrega todavia

- todas las fuentes funcionando
- informe final visual
- UI completa

## Alcance tecnico

### Entidades recomendadas

1. `BusinessProfile` (fuente de verdad del negocio)
2. `BusinessIdentity` (datos de identificacion y aliases)
3. `SourceProfile` (representacion del negocio en cada fuente)
4. `Mention` (modelo canonico de evidencia)
5. `ReviewMention` (subtipo con rating)
6. `EntityMatch` / `LinkEvidence` (por que se vincula una fuente a un negocio)

### Colecciones sugeridas (Mongo)

- `business_profiles`
- `source_profiles`
- `mentions`
- `entity_matches`
- `analysis_runs`
- `report_runs`

(Se puede convivir temporalmente con colecciones actuales mientras se migra.)

## Paso a paso (orden recomendado)

### Paso 1 - Definir el input de negocio ampliado

Diseñar payload de entrada para futuros endpoints/jobs:

- `query_name` (obligatorio)
- `address`
- `city`
- `country`
- `language`
- `website`
- `phone`
- `aliases[]`
- `categories[]` (si se conocen)

Regla: permitir payload minimo (solo nombre) pero soportar payload enriquecido sin friccion.

### Paso 2 - Diseñar identidad canonica del negocio

Separar dos capas:

- datos introducidos por usuario/sistema (input)
- datos confirmados por evidencia de fuentes (source truth consolidada)

Definir campos y politicas:

- normalizacion de texto
- idioma por defecto
- pais/ciudad normalizados
- versionado de cambios (quien/como se actualizo)

### Paso 3 - Diseñar `SourceProfile`

Un `SourceProfile` representa al negocio en una fuente concreta:

Campos recomendados:

- `business_id`
- `source` (`google_maps`, `tripadvisor`, `trustpilot`, `reddit`...)
- `source_profile_id`
- `url`
- `display_name`
- `address`
- `rating`
- `total_reviews`
- `categories`
- `match_confidence`
- `match_reason`
- `last_scraped_at`

Esto permite varias fichas potenciales por fuente y elegir la correcta con evidencia.

### Paso 4 - Diseñar el modelo canonico de evidencia (`Mention`)

No todo sera review. Reddit trae posts/comentarios; otras fuentes pueden traer menciones sin rating.

Definir estructura base:

- `business_id`
- `source`
- `source_item_id`
- `source_profile_id` (si aplica)
- `type` (`review`, `post`, `comment`, `mention`)
- `author`
- `published_at`
- `language`
- `text`
- `rating` (nullable)
- `engagement` (upvotes, replies, etc.)
- `url`
- `raw_payload_ref`
- `dedupe_fingerprint`

### Paso 5 - Entity matching (centralizacion por negocio)

Definir un score de matching multi-senal:

- nombre exacto / similar
- ciudad / pais
- direccion parcial
- website
- telefono
- categoria
- contexto de texto (si es foro/red social)

Resultado del matching:

- `accepted`
- `rejected`
- `needs_review` (si quieres revision manual posterior)

### Paso 6 - Dedupe y source truth

Diseñar deduplicacion en dos niveles:

1. Intra-fuente (mismo item repetido)
2. Inter-fuente (copias/reposts o syndication)

Definir como se actualiza el `BusinessProfile` consolidado cuando hay conflicto entre fuentes.

### Paso 7 - Migracion gradual desde modelo actual

No romper lo que ya tienes:

- mantener colecciones actuales mientras introduces nuevas
- crear adaptadores de lectura para seguir respondiendo endpoints actuales
- mapear reviews actuales de Google Maps al modelo canonico en paralelo

### Paso 8 - Validacion de la fase

Validar con Google Maps como fuente piloto:

- un negocio puede tener `BusinessProfile`
- un `SourceProfile` de Google Maps asociado
- mentions/reviews normalizadas al modelo canonico
- matching y dedupe basicos funcionando

## Entregables

- Esquema de datos canonico (documentado)
- Input de negocio enriquecido (nombre, direccion, ciudad, pais, idioma, etc.)
- Primer pipeline de centralizacion por negocio
- Compatibilidad temporal con el modelo actual

## Riesgos y mitigaciones

### Riesgo 1 - Sobreingenieria del modelo

Mitigacion: empezar con campos nucleares y permitir extensiones por `meta`.

### Riesgo 2 - Matching incorrecto entre fuentes

Mitigacion: usar score + evidencia + umbral + trazabilidad de decision.

### Riesgo 3 - Migracion dolorosa de datos

Mitigacion: migracion paralela y adapters de compatibilidad durante varias iteraciones.

## Criterios de salida

- Existe un `BusinessProfile` enriquecido como fuente de verdad.
- Existe `SourceProfile` y modelo canonico de `Mention`/`ReviewMention`.
- Google Maps puede mapearse al nuevo modelo sin romper el flujo actual.
- El sistema queda listo para integrar una nueva fuente (Fase 3).

## Dependencia con la siguiente fase

La Fase 3 (Tripadvisor) debe usar este modelo desde el inicio para no crear otro silo de datos.
