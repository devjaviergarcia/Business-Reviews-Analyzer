# V223 — Optimización SEO para Google AI Search (Repiq)

## Fuentes usadas
- Transcripción interna: `docs/seo/video seo google.txt`.
- Guía oficial de Google Search: https://developers.google.com/search/docs/fundamentals/ai-optimization-guide

## Resumen ejecutivo
Google deja claro que la optimización para AI Overviews y AI Mode sigue siendo SEO. Para Repiq, el foco debe ser: contenido no commodity con punto de vista propio, estructura técnica limpia e indexable, cobertura temática lateral (query fanout), y distribución externa auténtica de menciones.

## Prioridades

### P0 (esta semana)
1. Respuesta-first content en landing:
- Abrir con una respuesta clara a "qué hace Repiq" y "qué resultado obtiene el negocio".
- Mantener jerarquía semántica estable (`H1` único + `H2/H3` por bloques).

2. Extractabilidad para sistemas generativos:
- FAQ explícita y legible en HTML.
- Bloques con respuestas directas (párrafos cortos, contexto claro).

3. Indexabilidad técnica:
- Sitemap con páginas estratégicas (`/`, `/insights`, `/insights/*`).
- Robots coherente (permitir secciones públicas y bloquear solo privadas/técnicas).

4. Entidades y contexto:
- Añadir JSON-LD (`Service`, `FAQPage`, `Article`) para mejorar comprensión de contenido y elegibilidad de rich features.

### P1 (próximas 2-3 semanas)
1. Query fanout editorial:
- Publicar piezas por clúster de intención (no por keyword exacta repetida).
- Cada pieza debe cubrir intención principal + subconsultas laterales.

2. Frescura controlada:
- Revisar/actualizar contenidos clave con fecha visible y cambios reales mensuales.

3. Enlazado interno funcional:
- Desde landing a insights.
- Desde insights hacia CTA de solicitud de estudio con UTMs coherentes.

### P2 (siguiente fase)
1. Señales externas de marca:
- Distribución de insights en LinkedIn y comunidades sectoriales con discusión real (sin menciones artificiales).
- Citas y colaboraciones con fuentes locales/sectoriales.

2. Medición SEO/AI:
- Consola de búsqueda: monitorizar queries de descubrimiento, CTR de páginas insights y cobertura de indexación.
- Revisión trimestral de clústeres temáticos con rendimiento bajo.

## Checklist técnico on-page (landing)

### Indexabilidad
- [x] URL principal indexable.
- [x] `sitemap.xml` con páginas estratégicas y editoriales.
- [x] `robots.txt` permite secciones públicas y bloquea endpoints técnicos.

### Jerarquía semántica
- [x] Un `H1` principal en home.
- [x] Secciones con `H2` y tarjetas/bloques con `H3`.
- [x] FAQ con preguntas explícitas en HTML legible.

### Extractabilidad de respuestas
- [x] Respuestas directas cerca de encabezados.
- [x] Párrafos cortos con intención explícita.
- [x] FAQ modelada también como `FAQPage` (JSON-LD).

### Enlazado interno
- [x] Landing enlaza a `/insights`.
- [x] Cada insight enlaza a CTA de solicitud con `utm_source=seo`.

## Plan editorial (contenido único + query fanout)

### Clúster 1 — Reputación local en hostelería
- Pieza: `estado-resenas-restaurantes-cordoba`
- Intento principal: interpretación de reseñas y señales que mueven decisión.
- Fanout: comparación por zona, reseñas negativas, volumen vs calidad.

### Clúster 2 — Competencia local en Google Maps
- Pieza: `competencia-google-maps-restaurantes-cordoba`
- Intento principal: cómo comparar sin ruido.
- Fanout: capas de competencia, errores de lectura, prioridades de acción.

### Clúster 3 — Mejora de nota y confianza
- Pieza: `como-subir-nota-google-negocio-local`
- Intento principal: subir valoración sin prácticas de riesgo.
- Fanout: cadencia operativa, respuesta a críticas, calidad de señal.

## Plan de distribución externa (menciones auténticas)
- LinkedIn (perfil fundador + empresa): 1 publicación semanal por insight con dato concreto y CTA a estudio.
- Comunidades sectoriales (hostelería/local business): republicar resumen accionable y abrir debate.
- Directorios/medios locales: proponer cápsulas con datos agregados por ciudad/sector.
- Formato recomendado por pieza: 1 insight -> 1 post corto + 1 carrusel + 1 comentario-resumen fijado.

## Mitos descartados (alineado con Google)
- No crear `llms.txt` ni marcado especial para AI Search.
- No trocear contenido artificialmente solo por "chunking".
- No generar variantes masivas para cada long-tail sin valor real.
- No perseguir menciones externas inauténticas.

## Implementación aplicada en V223
Se han realizado cambios concretos en la landing y editorial para cumplir este ticket:
- Secciones SEO en home con respuesta-first y FAQ.
- JSON-LD para `Service` y `FAQPage` en home.
- Biblioteca editorial `/insights` con artículos `Article` + enlazado a CTA.
- Sitemap ampliado con rutas editoriales.

## Próximo paso operativo
Publicar semanalmente los insights ya creados en LinkedIn con UTM y monitorizar en Search Console qué consultas nuevas empiezan a traccionar para ampliar fanout sin duplicar contenido.
