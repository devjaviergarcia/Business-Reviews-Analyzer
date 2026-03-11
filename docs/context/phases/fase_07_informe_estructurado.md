# Fase 7 - Informe estructurado (PDF / Typst / plantillas)

## Objetivo

Generar un informe profesional, estructurado y trazable a partir del analisis realizado, listo para compartir con un empresario.

Esta fase transforma el analisis JSON en un artefacto de producto.

## Problema que resuelve

Hoy existe analisis por API, pero no un informe final consistente y presentable. Para vender/operar demos y uso real hace falta:

- formato visual claro
- narrativa ejecutiva
- evidencia trazable
- recomendaciones accionables ordenadas

## Alcance de producto

### Lo que SI se entrega

- estructura formal de informe
- generacion automatica de informe en formato exportable (PDF/Typst/HTML->PDF)
- secciones estandarizadas
- resumen ejecutivo + detalle + acciones propuestas

### Lo que NO se entrega aun

- UI final completa (llega en la siguiente fase)
- automatizacion comercial de landing (fase posterior)

## Decisiones de formato (a decidir en esta fase)

Opciones validas:

- **Typst** (muy buena opcion para documentos estructurados/versionables)
- HTML/CSS + render a PDF
- PDF generado por libreria (menos flexible visualmente)

Recomendacion pragmatica:

1. definir primero un schema de informe (`ReportModel`)
2. generar HTML/JSON intermedio
3. evaluar Typst como render final si encaja con velocidad de desarrollo

## Estructura sugerida del informe

1. Portada (negocio, fecha, periodo analizado, fuentes)
2. Resumen ejecutivo (1 pagina)
3. Salud reputacional general
4. Analisis por fuente
5. Temas principales (positivos/negativos)
6. Problemas prioritarios y oportunidades
7. Recomendaciones accionables (priorizadas)
8. Respuestas sugeridas (si aplica)
9. Anexo de evidencia / metodologia

## Paso a paso (orden recomendado)

### Paso 1 - Definir `ReportModel` (contrato estructurado)

Diseñar un modelo de reporte independiente del render:

- `report_id`
- `business_profile`
- `analysis_run_ids`
- `summary`
- `source_sections`
- `topic_sections`
- `action_plan`
- `owner_reply_samples`
- `methodology`
- `appendix`

Esto desacopla el contenido del formato final.

### Paso 2 - Diseñar plantillas de contenido por modo de analisis

No todos los informes deben tener la misma profundidad.

Definir al menos:

- informe demo (breve)
- informe completo (profundo)

Cada uno con:

- secciones obligatorias
- secciones opcionales
- limites de longitud
- tono de redaccion

### Paso 3 - Implementar `report-worker`

Responsabilidades:

- consumir `AnalysisResult`
- construir `ReportModel`
- renderizar artefacto final
- persistir resultado y metadatos
- emitir eventos de progreso

### Paso 4 - Render estructurado (fase tecnica)

Implementar pipeline de render:

1. `ReportModel` -> template context
2. template -> documento intermedio (HTML o Typst)
3. intermedio -> PDF (o salida final)
4. almacenamiento del artefacto (`report_runs` + file path/url)

### Paso 5 - Trazabilidad y evidencia

El informe no debe parecer inventado.

Introducir referencias internas a evidencia:

- conteos por fuente
- citas/resumenes anonimizados (si aplica)
- top issues por frecuencia/impacto
- notas de confianza/limitaciones

### Paso 6 - Calidad visual y consistencia

Definir guia minima:

- tipografia
- bloques y jerarquia visual
- tablas y graficos simples
- colores semanticos (positivo/neutral/negativo)

Objetivo: profesional y claro, no "dashboard ruidoso".

### Paso 7 - Validacion de informes

Comprobar:

- integridad de datos (no secciones vacias sin control)
- consistencia numerica (totales, fuentes)
- tiempos de generacion
- peso del PDF
- legibilidad para un empresario no tecnico

## Entregables

- `ReportModel` versionado
- `report-worker` operativo
- Plantillas de informe (demo/completo)
- Generacion de PDF/Typst/HTML exportable
- Persistencia de `report_runs` y artefactos

## Riesgos y mitigaciones

- **Acoplar reporte al output LLM textual**: usar `ReportModel` estructurado.
- **Exceso de texto poco accionable**: secciones y limites claros por plantilla.
- **Formato inestable**: template tests con snapshots de render.

## Criterios de salida

- Se puede generar un informe estructurado real desde un `AnalysisRun`.
- El informe es consistente y entendible por un empresario.
- La salida del `report-worker` queda lista para mostrarse en UI (Fase 8).

## Dependencia con la siguiente fase

La Fase 8 usara este informe ya generado para mostrar resultados en una interfaz MVP sin mezclar rendering complejo en frontend.
