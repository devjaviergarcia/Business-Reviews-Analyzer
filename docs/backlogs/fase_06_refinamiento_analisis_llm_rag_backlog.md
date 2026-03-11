# Backlog FASE_06 - Refinamiento de analisis LLM y RAG

- `fase`: `fase_06`
- `documento de fase`: `docs/context/phases/fase_06_refinamiento_analisis_llm_rag.md`
- `archivo backlog`: `docs/backlogs/fase_06_refinamiento_analisis_llm_rag_backlog.md`
- `epicas`: `4`
- `tickets`: `12`
- `estado general sugerido`: `TODO`

## Objetivo operativo

mejorar calidad y control del analisis con modos, prompts versionados, batchers, reanalyze y RAG

## Contexto y foco de trabajo

Esta fase ataca el siguiente objetivo del roadmap: **mejorar calidad y control del analisis con modos, prompts versionados, batchers, reanalyze y RAG**.

Palabras clave de implementacion/seguimiento: `analysis_mode, prompt versioning, reanalyze, batching, RAG, coste, latencia`.

## Secuencia recomendada

1. Definir modos y contratos de salida primero.
2. Versionar prompts y medir calidad con dataset.
3. Usar reanalyze sobre datos guardados para iterar rapido.

## Criterios de done de la fase

1. modos de analisis definidos y seleccionables
2. prompts versionados y metadata persistida
3. reanalyze con batchers configurable
4. metricas de coste/latencia y fallbacks

## Resumen de epicas

| ID | Epica | Tickets | Resultado esperado |
| --- | --- | ---: | --- |
| `E06-01` | Modos de analisis y contratos de salida | 3 | La fase 'Refinamiento de analisis LLM y RAG' tiene alcance, contratos y validaciones definidos. |
| `E06-02` | Prompts versionados y evaluacion | 3 | Existe una implementacion funcional del bloque principal de la fase 'Refinamiento de analisis LLM y RAG'. |
| `E06-03` | Batching, reanalyze avanzado y RAG minimo | 3 | La fase puede operarse con visibilidad y diagnostico suficiente. |
| `E06-04` | Coste, latencia y fallbacks operativos | 3 | La fase 'Refinamiento de analisis LLM y RAG' cierra con criterio objetivo y backlog de siguientes mejoras. |

## Epicas y tickets

### E06-01 - Modos de analisis y contratos de salida

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase 'Refinamiento de analisis LLM y RAG' tiene alcance, contratos y validaciones definidos.

**Objetivo de la epica**

Bajar a contratos y decisiones tecnicas el objetivo de la fase: mejorar calidad y control del analisis con modos, prompts versionados, batchers, reanalyze y RAG.

**Riesgos a vigilar**

- SobrediseÃ±ar antes de validar el flujo real.
- No definir validacion temprana y llegar al final con incertidumbre.

**Tickets**

#### T06-001 - DiseÃ±ar catalogo de modos (`demo`, `deep`, `sentiment`, `solutions`)

- `tipo`: `Diseno`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Especificar que datos entran, que artefactos salen y como se valida el resultado de la fase. Contexto: analysis_mode, prompt versioning, reanalyze, batching, RAG, coste, latencia.

**Criterios de aceptacion**

1. Entradas y salidas definidas con ejemplos.
2. Precondiciones y dependencias explicitadas.
3. Criterio de validacion funcional acordado.

**Validacion / prueba**

- Revision tecnica del contrato contra el roadmap y el codigo actual.

#### T06-002 - Definir DTOs/esquemas de salida por modo

- `tipo`: `QA`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Definir pruebas minimas para validar la fase sin esperar al final: smoke, fixtures, pruebas de integracion y chequeos manuales.

**Dependencias**

- `T06-001`

**Criterios de aceptacion**

1. Existe checklist de pruebas de la fase.
2. Cada prueba tiene comando/precondiciones.
3. Se definen criterios de exito/fallo.

**Validacion / prueba**

- Ejecucion de al menos una prueba de referencia o simulacion del flujo.

#### T06-003 - Exponer `analysis_mode` en analyze/reanalyze/jobs

- `tipo`: `Documentacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Registrar los limites esperados de la fase y riesgos principales para Refinamiento de analisis LLM y RAG, incluyendo deuda tecnica aceptada temporalmente.

**Dependencias**

- `T06-001`

**Criterios de aceptacion**

1. Lista de riesgos y mitigaciones iniciales.
2. Supuestos tecnicos explicitados.
3. Deuda tecnica temporal identificada.

**Validacion / prueba**

- Revision documental por consistencia con fase/contexto.

### E06-02 - Prompts versionados y evaluacion

- `estado sugerido`: `TODO`

**Resultado esperado**

Existe una implementacion funcional del bloque principal de la fase 'Refinamiento de analisis LLM y RAG'.

**Objetivo de la epica**

Construir el nucleo tecnico que materializa la fase: mejorar calidad y control del analisis con modos, prompts versionados, batchers, reanalyze y RAG.

**Dependencias**

- `E06-01`

**Riesgos a vigilar**

- Acoplamiento excesivo con implementacion actual.
- Cambios internos rompiendo endpoints/contratos existentes.

**Tickets**

#### T06-004 - Externalizar prompts con versionado

- `tipo`: `Implementacion`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Desarrollar la pieza central de la fase (modulos, servicios, conectores o UI) manteniendo separacion de responsabilidades. Pistas: analysis_mode, prompt versioning, reanalyze, batching, RAG, coste, latencia.

**Dependencias**

- `T06-001`

**Criterios de aceptacion**

1. La funcionalidad principal existe y se ejecuta.
2. Se respeta separacion de capas/modulos razonable.
3. El codigo sigue contratos definidos en la fase.

**Validacion / prueba**

- Smoke test del flujo principal de la fase.

#### T06-005 - Crear dataset de evaluacion y rubric

- `tipo`: `Implementacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Exponer configuracion minima de timeouts, limites, modos o estrategias para poder operar y depurar sin tocar codigo continuamente.

**Dependencias**

- `T06-004`

**Criterios de aceptacion**

1. Parametros principales centralizados.
2. Defaults razonables definidos.
3. Se pueden sobreescribir para pruebas.

**Validacion / prueba**

- Probar ejecucion con defaults y con una variacion de parametros.

#### T06-006 - Persistir metadata de analisis (prompt/modelo/batcher)

- `tipo`: `Integracion`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Conectar la nueva funcionalidad con servicios, jobs, endpoints o workers ya existentes sin romper compatibilidad externa.

**Dependencias**

- `T06-004`

**Criterios de aceptacion**

1. La integracion no rompe el baseline.
2. Estados/eventos/respuestas reflejan la nueva fase.
3. Se documenta el flujo de integracion.

**Validacion / prueba**

- E2E o integracion controlada con el sistema actual.

### E06-03 - Batching, reanalyze avanzado y RAG minimo

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase puede operarse con visibilidad y diagnostico suficiente.

**Objetivo de la epica**

AÃ±adir logs, eventos, metricas y manejo de errores utiles para operar la fase y compararla con versiones futuras.

**Dependencias**

- `E06-02`

**Riesgos a vigilar**

- Falta de visibilidad real del comportamiento en runtime.
- Errores silenciosos o mensajes poco accionables.

**Tickets**

#### T06-007 - DiseÃ±ar estrategias de batching por volumen/modo

- `tipo`: `Observabilidad`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Emitir eventos y logs estructurados con etapa, conteos y errores para depuracion y seguimiento de progreso.

**Dependencias**

- `T06-006`

**Criterios de aceptacion**

1. Etapas clave emiten informacion de progreso.
2. Errores incluyen contexto suficiente.
3. Logs/eventos son consistentes con el resto del sistema.

**Validacion / prueba**

- Revisar logs/eventos en una ejecucion real de la fase.

#### T06-008 - Implementar reanalyze configurable y comparativas

- `tipo`: `Observabilidad`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Registrar metricas operativas (tiempo, volumen, exito/fallo, cobertura) para medir la fase y comparar iteraciones.

**Dependencias**

- `T06-007`

**Criterios de aceptacion**

1. Metricas minimas definidas y emitidas.
2. Se pueden consultar en logs/DB/eventos.
3. Sirven para detectar regresiones.

**Validacion / prueba**

- Ejecutar una corrida y revisar metricas registradas.

#### T06-009 - Implementar RAG minimo para modos de profundidad

- `tipo`: `Bugfix/Implementacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Tipar errores frecuentes y definir que es recuperable vs no recuperable para mejorar estabilidad operativa.

**Dependencias**

- `T06-007`

**Criterios de aceptacion**

1. Errores comunes generan mensajes accionables.
2. Se aplican retries/fallbacks donde tenga sentido.
3. Fallos no dejan recursos abiertos o estados inconsistentes.

**Validacion / prueba**

- Provocar 1-2 fallos controlados y revisar comportamiento.

### E06-04 - Coste, latencia y fallbacks operativos

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase 'Refinamiento de analisis LLM y RAG' cierra con criterio objetivo y backlog de siguientes mejoras.

**Objetivo de la epica**

Definir criterio de done, resultados medidos y pendientes priorizados para la siguiente fase o iteracion.

**Dependencias**

- `E06-03`

**Riesgos a vigilar**

- Cerrar la fase sin criterios objetivos.
- No capturar deuda tecnica y repetir errores en la siguiente fase.

**Tickets**

#### T06-010 - Registrar metricas de coste/tokens/latencia

- `tipo`: `QA`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Correr la validacion final definida para la fase y capturar resultados, evidencias y problemas encontrados.

**Dependencias**

- `T06-002`
- `T06-006`
- `T06-008`

**Criterios de aceptacion**

1. Se ejecuta validacion final definida.
2. Resultados y evidencias quedan registrados.
3. Problemas se convierten en tickets o deuda.

**Validacion / prueba**

- Checklist de salida completada.

#### T06-011 - Implementar fallbacks por cuota/coste/timeout

- `tipo`: `Documentacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Actualizar docs de fase/proyecto con lo implementado, limites conocidos y decisiones tomadas.

**Dependencias**

- `T06-010`

**Criterios de aceptacion**

1. Docs de fase y/o arquitectura actualizados.
2. Se listan limites y decisiones de diseÃ±o.
3. Se enlazan artefactos y pruebas relevantes.

**Validacion / prueba**

- Revision documental final.

#### T06-012 - Definir calidad minima por modo para produccion

- `tipo`: `Planificacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Cerrar la fase dejando pendientes priorizados, con impacto y dependencias claras para la siguiente iteracion.

**Dependencias**

- `T06-010`
- `T06-011`

**Criterios de aceptacion**

1. Pendientes priorizados y categorizados.
2. Se identifican riesgos si se difieren.
3. La siguiente fase recibe inputs claros.

**Validacion / prueba**

- Revision del backlog de continuidad contra roadmap.
