# Fase 5 - Integracion scraper/API de Reddit

## Objetivo

Integrar Reddit como fuente de menciones (posts y comentarios) relacionadas con el negocio, su zona o su categoria, utilizando API oficial cuando sea posible.

## Por que Reddit es una fase separada

Reddit no es una fuente de "reviews" clasicas. Aporta:

- menciones organicas
- comparativas con competidores
- problemas contextuales (zona, servicio, experiencia)
- recomendaciones y anti-recomendaciones

Esto obliga a ampliar el enfoque desde review estructurada a `Mention` canonica.

## Valor de negocio de esta fase

- detectar percepcion no capturada en reseñas formales
- descubrir temas emergentes o reputacionales
- entender contexto local y comparativas

## Alcance de producto

### Lo que SI se entrega

- busqueda de posts/comentarios relevantes por negocio y contexto geografico
- normalizacion a `Mention` / `ReviewMention` (si hubiera rating, normalmente no)
- integracion en analisis multi-fuente

### Lo que NO se entrega aun

- modelo social completo multi-red
- social listening en tiempo real
- panel de moderacion

## Alcance tecnico

### Recomendacion principal

Usar API oficial de Reddit (o proveedor oficial/autorizado) antes que scraping de HTML.

Ventajas:

- mayor estabilidad
- menor mantenimiento de selectores
- mejor control de rate limits
- metadatos utiles (score, replies, timestamps)

### Retos especificos

- matching semantico mas complejo (menciones ambiguas)
- mucho ruido no relacionado con el negocio
- sarcasmo/ironia/contexto en lenguaje natural
- falta de rating explicito

## Paso a paso (orden recomendado)

### Paso 1 - Definir estrategia de consulta

Diseñar consultas por capas:

1. nombre exacto del negocio
2. nombre + ciudad
3. aliases + barrio/ciudad
4. categoria + ciudad (para contexto competitivo)

Definir ventanas temporales y limites por consulta para no disparar coste/ruido.

### Paso 2 - Implementar cliente Reddit

1. Crear adaptador `sources/reddit/`.
2. Configurar credenciales/API.
3. Implementar cliente de lectura (posts y comentarios).
4. Gestionar rate limits y retries.

### Paso 3 - Pipeline de recoleccion de evidencia

Para cada consulta:

- recuperar posts candidatos
- recuperar comentarios relevantes
- guardar raw payload
- etiquetar `query_context` (por que llego esa evidencia)

### Paso 4 - Filtro de relevancia y matching

Aplicar filtros por etapas:

1. Filtro lexical (nombre/alias/ciudad)
2. Filtro heuristico (subreddit, contexto geografico, categoria)
3. Score de matching con `BusinessProfile`
4. Rechazo de ruido evidente

Guardar score y razon de aceptacion/rechazo.

### Paso 5 - Normalizacion a `Mention`

Mapear posts y comentarios a un modelo canonico:

- `type = post` o `comment`
- `author`
- `published_at`
- `text`
- `engagement` (score, replies)
- `url`
- `source_item_id`
- `language`
- `match_confidence`

No forzar rating si no existe.

### Paso 6 - Integracion en analisis

Definir como participa Reddit en el pipeline:

- en analisis demo: muestra menciones destacadas
- en analisis profundo: contribuye a temas/problemas emergentes
- en analisis de sentimientos: cuenta como evidencia textual sin rating
- en analisis de soluciones: aporta dolores y expectativas expresadas

### Paso 7 - Controles de calidad

Validar calidad de relevancia:

- precision de matching (cuantas menciones realmente eran del negocio)
- ruido por subreddit generalista
- utilidad real de insights para negocio

## Entregables

- Integracion Reddit (API preferred)
- Pipeline de recoleccion de posts/comentarios
- Filtro de relevancia y matching trazable
- Normalizacion a `Mention`
- Integracion en pipeline de analisis multi-fuente

## Riesgos y mitigaciones

- **Mucho ruido**: filtros y scoring en varias capas.
- **Ambiguedad de nombres**: exigir ciudad/pais/aliases cuando se pueda.
- **Coste de analisis de texto**: limitar muestra y aplicar priorizacion por engagement.

## Criterios de salida

- Reddit aporta menciones relevantes con precision aceptable.
- Las menciones se integran al modelo canonico sin romper el pipeline de reviews.
- El sistema puede analizar negocio con Google + (Tripadvisor/Trustpilot) + Reddit como señales complementarias.

## Dependencia con la siguiente fase

La Fase 6 usa esta mezcla de fuentes estructuradas y no estructuradas para refinar prompts, RAG y modos de analisis.
