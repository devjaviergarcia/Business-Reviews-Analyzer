# Fase 6 - Refinamiento del analisis (LLM, prompting, RAG y modos)

## Objetivo

Elevar la calidad, consistencia y utilidad del analisis, pasando de un output general a un sistema de analisis configurable por modo de negocio.

Esta fase convierte el pipeline LLM en un motor mas serio y controlado.

## Problema que resuelve

Aunque el analisis actual funciona, puede ser pobre o irregular porque:

- depende de un prompt unico y general
- la muestra de reviews puede no representar bien el negocio
- no existe un sistema de modos de analisis (demo/profundo/etc.)
- la salida no esta totalmente estructurada para reporting posterior

## Objetivos de producto

Introducir distintos modos de analisis para distintos usos:

- `demo`: rapido, barato, convincente para primera toma de contacto
- `profundo`: mas contexto, mas cobertura y mas detalle
- `solo_sentimientos`: rapido para monitorizacion basica
- `soluciones`: enfocado a acciones concretas y mejoras recomendadas

## Alcance tecnico

### Areas a trabajar

1. Prompting y contratos de salida
2. Muestreo/batching de evidencia
3. RAG/context enrichment (si aplica)
4. Post-procesado y validacion de outputs
5. Coste/latencia/observabilidad de LLM

## Paso a paso (orden recomendado)

### Paso 1 - Definir taxonomia de analisis

Para cada modo de analisis definir:

- objetivo de negocio
- inputs minimos requeridos
- tiempo/coste esperado
- salida esperada (campos obligatorios)
- criterios de calidad

Esto evita que "modo" sea solo un string sin implicaciones reales.

### Paso 2 - Rediseñar el contrato de salida del analisis

Definir schema versionado para `AnalysisRun`:

- `analysis_type`
- `analysis_version`
- `overall_sentiment`
- `confidence`
- `topics`
- `strengths`
- `weaknesses`
- `opportunities`
- `risks`
- `recommended_actions`
- `suggested_owner_replies`
- `evidence_summary`
- `source_breakdown`
- `meta` (coste, tiempo, prompts, samples)

Regla: el output debe ser consumible por el `report-worker` sin parseos frágiles.

### Paso 3 - Mejorar estrategia de muestreo/batching

Evolucionar desde batches simples a estrategias por objetivo:

- balanceado por rating
- recencia
- volumen por fuente
- relevancia textual
- criticidad (menciones negativas/alto engagement)

Para cada modo de analisis, definir una politica de muestra diferente.

### Paso 4 - Diseñar RAG / contexto adicional

No todo requiere RAG pesado, pero si contexto util:

- metadata del negocio (ciudad, categoria, idioma)
- source profiles (ratings por fuente)
- resumen estadistico previo
- ejemplos de problemas recurrentes historicos

Opciones:

- RAG ligero con contexto estructurado en prompt
- retrieval por embeddings de menciones/reviews (fase posterior si se necesita)

Objetivo de esta fase: **organizar el contexto**, no necesariamente desplegar un sistema vectorial complejo si no aporta valor inmediato.

### Paso 5 - Plantillas de prompts por modo

Crear prompts separados (versionados) para:

- demo
- profundo
- solo_sentimientos
- soluciones

Cada prompt debe especificar:

- tono (analitico, no marketing)
- formato de salida JSON estricto
- prioridades (accionabilidad, claridad, evidencia)
- limites (no inventar datos, citar incertidumbre)

### Paso 6 - Validacion y post-procesado

Introducir capa de validacion del output LLM:

- schema validation
- normalizacion de listas/labels
- dedupe de topics y acciones
- fallback controlado si JSON invalido
- scoring interno de calidad del resultado

### Paso 7 - Observabilidad de analisis

Persistir metadatos para optimizacion futura:

- latencia por modo
- numero de items analizados
- tokens/coste estimado
- errores de parsing
- calidad score interno
- provider/model usado

Esto permitira afinar prompts con datos y no por sensacion.

### Paso 8 - Pruebas de calidad de analisis

Crear benchmark interno con varios negocios y comparar:

- coherencia del output
- accionabilidad
- estabilidad entre ejecuciones
- utilidad para reporte y UI

## Entregables

- Modos de analisis versionados (`demo`, `profundo`, `solo_sentimientos`, `soluciones`)
- Prompts y contratos de salida por modo
- Estrategias de muestreo por modo/fuente
- Validacion y post-procesado robusto del output LLM
- Metricas de coste/latencia/calidad del analisis

## Riesgos y mitigaciones

- **Prompts demasiado complejos**: dividir por modos y schemas claros.
- **Coste alto**: muestras adaptativas y modos rapidos para demo.
- **Outputs inconsistentes**: validacion fuerte + fallback + versionado de prompt.

## Criterios de salida

- Existen modos de analisis con diferencias reales y documentadas.
- El output esta estructurado para consumo por `report-worker`.
- Se puede medir calidad/coste/latencia del analisis.

## Dependencia con la siguiente fase

La Fase 7 usa estos outputs estructurados para generar informes profesionales sin reconstruir la logica de negocio en la capa de reporte.
