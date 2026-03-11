# Backlog FASE_04 - Scraper Trustpilot

- `fase`: `fase_04`
- `documento de fase`: `docs/context/phases/fase_04_scraper_trustpilot.md`
- `archivo backlog`: `docs/backlogs/fase_04_scraper_trustpilot_backlog.md`
- `epicas`: `4`
- `tickets`: `12`
- `estado general sugerido`: `TODO`

## Objetivo operativo

implementar conector vertical de Trustpilot integrado en la pipeline y normalizado al modelo canonico

## Contexto y foco de trabajo

Esta fase ataca el siguiente objetivo del roadmap: **implementar conector vertical de Trustpilot integrado en la pipeline y normalizado al modelo canonico**.

Palabras clave de implementacion/seguimiento: `trustpilot, dominio web, parser, normalizador, pipeline`.

## Secuencia recomendada

1. Priorizar coincidencia por dominio web cuando exista.
2. Separar discovery/matching del parser.
3. Controlar limites y bloqueos desde configuracion.

## Criterios de done de la fase

1. discovery y matching minimo funcional
2. extraccion y normalizacion al modelo canonico
3. integracion con jobs y eventos
4. smoke tests y limites operativos documentados

## Resumen de epicas

| ID | Epica | Tickets | Resultado esperado |
| --- | --- | ---: | --- |
| `E04-01` | Discovery y matching de Trustpilot | 3 | El sistema localiza la entidad correcta en Trustpilot y la valida con matching explicable. |
| `E04-02` | Parser y normalizador de Trustpilot | 3 | El conector `trustpilot` extrae datos de Trustpilot y los normaliza al modelo canonico. |
| `E04-03` | Robustez operativa y observabilidad de Trustpilot | 3 | La fase puede operarse con visibilidad y diagnostico suficiente. |
| `E04-04` | Integracion E2E de `trustpilot` en la pipeline | 3 | Trustpilot participa en jobs de scraping/analisis sin romper el baseline existente. |

## Epicas y tickets

### E04-01 - Discovery y matching de Trustpilot

- `estado sugerido`: `TODO`

**Resultado esperado**

El sistema localiza la entidad correcta en Trustpilot y la valida con matching explicable.

**Objetivo de la epica**

Definir y validar discovery + entity matching en Trustpilot, usando como seÃ±al fuerte dominio web oficial del negocio.

**Riesgos a vigilar**

- SobrediseÃ±ar antes de validar el flujo real.
- No definir validacion temprana y llegar al final con incertidumbre.

**Tickets**

#### T04-001 - DiseÃ±ar flujo de discovery en Trustpilot

- `tipo`: `Diseno`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Documentar como localizar la ficha/contenido objetivo en Trustpilot: busqueda, filtros, seleccion y seÃ±ales de identidad. SeÃ±al fuerte: dominio web oficial del negocio.

**Criterios de aceptacion**

1. Entradas y salidas definidas con ejemplos.
2. Precondiciones y dependencias explicitadas.
3. Criterio de validacion funcional acordado.

**Validacion / prueba**

- Revision tecnica del contrato contra el roadmap y el codigo actual.

#### T04-002 - Implementar cliente/flujo de acceso para Trustpilot

- `tipo`: `QA`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Crear `browser_flow.py` o `api_client.py` para Trustpilot, separado del parser, que devuelva payload bruto y metadatos de discovery.

**Dependencias**

- `T04-001`

**Criterios de aceptacion**

1. Existe checklist de pruebas de la fase.
2. Cada prueba tiene comando/precondiciones.
3. Se definen criterios de exito/fallo.

**Validacion / prueba**

- Ejecucion de al menos una prueba de referencia o simulacion del flujo.

#### T04-003 - Implementar matching minimo para `trustpilot`

- `tipo`: `Documentacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Aplicar el servicio de matching sobre candidatos de Trustpilot y registrar `match_score`, decision y motivo resumido.

**Dependencias**

- `T04-001`

**Criterios de aceptacion**

1. Lista de riesgos y mitigaciones iniciales.
2. Supuestos tecnicos explicitados.
3. Deuda tecnica temporal identificada.

**Validacion / prueba**

- Revision documental por consistencia con fase/contexto.

### E04-02 - Parser y normalizador de Trustpilot

- `estado sugerido`: `TODO`

**Resultado esperado**

El conector `trustpilot` extrae datos de Trustpilot y los normaliza al modelo canonico.

**Objetivo de la epica**

Separar parser estructural y normalizador para `trustpilot` evitando selectores fragiles.

**Dependencias**

- `E04-01`

**Riesgos a vigilar**

- Acoplamiento excesivo con implementacion actual.
- Cambios internos rompiendo endpoints/contratos existentes.

**Tickets**

#### T04-004 - Implementar parser estructural para Trustpilot

- `tipo`: `Implementacion`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Construir parser basado en patrones estructurales/semanticos en Trustpilot, con fallbacks, evitando IDs dinamicos siempre que sea posible.

**Dependencias**

- `T04-001`

**Criterios de aceptacion**

1. La funcionalidad principal existe y se ejecuta.
2. Se respeta separacion de capas/modulos razonable.
3. El codigo sigue contratos definidos en la fase.

**Validacion / prueba**

- Smoke test del flujo principal de la fase.

#### T04-005 - Implementar normalizador `trustpilot` -> Mention/Review

- `tipo`: `Implementacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Mapear campos de Trustpilot al modelo canonico (`source`, `source_item_id`, `url`, `text`, `rating/engagement`, `raw_payload`).

**Dependencias**

- `T04-004`

**Criterios de aceptacion**

1. Parametros principales centralizados.
2. Defaults razonables definidos.
3. Se pueden sobreescribir para pruebas.

**Validacion / prueba**

- Probar ejecucion con defaults y con una variacion de parametros.

#### T04-006 - Persistir corrida de `trustpilot` con trazabilidad

- `tipo`: `Integracion`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Conectar la nueva funcionalidad con servicios, jobs, endpoints o workers ya existentes sin romper compatibilidad externa.

**Dependencias**

- `T04-004`

**Criterios de aceptacion**

1. La integracion no rompe el baseline.
2. Estados/eventos/respuestas reflejan la nueva fase.
3. Se documenta el flujo de integracion.

**Validacion / prueba**

- E2E o integracion controlada con el sistema actual.

### E04-03 - Robustez operativa y observabilidad de Trustpilot

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase puede operarse con visibilidad y diagnostico suficiente.

**Objetivo de la epica**

AÃ±adir logs, eventos, metricas y manejo de errores utiles para operar la fase y compararla con versiones futuras.

**Dependencias**

- `E04-02`

**Riesgos a vigilar**

- Falta de visibilidad real del comportamiento en runtime.
- Errores silenciosos o mensajes poco accionables.

**Tickets**

#### T04-007 - Configurar limites/retries/volumen para `trustpilot`

- `tipo`: `Observabilidad`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Emitir eventos y logs estructurados con etapa, conteos y errores para depuracion y seguimiento de progreso.

**Dependencias**

- `T04-006`

**Criterios de aceptacion**

1. Etapas clave emiten informacion de progreso.
2. Errores incluyen contexto suficiente.
3. Logs/eventos son consistentes con el resto del sistema.

**Validacion / prueba**

- Revisar logs/eventos en una ejecucion real de la fase.

#### T04-008 - Emitir eventos por etapas del conector `trustpilot`

- `tipo`: `Observabilidad`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Registrar metricas operativas (tiempo, volumen, exito/fallo, cobertura) para medir la fase y comparar iteraciones.

**Dependencias**

- `T04-007`

**Criterios de aceptacion**

1. Metricas minimas definidas y emitidas.
2. Se pueden consultar en logs/DB/eventos.
3. Sirven para detectar regresiones.

**Validacion / prueba**

- Ejecutar una corrida y revisar metricas registradas.

#### T04-009 - Documentar limites y riesgos de uso de Trustpilot

- `tipo`: `Bugfix/Implementacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Tipar errores frecuentes y definir que es recuperable vs no recuperable para mejorar estabilidad operativa.

**Dependencias**

- `T04-007`

**Criterios de aceptacion**

1. Errores comunes generan mensajes accionables.
2. Se aplican retries/fallbacks donde tenga sentido.
3. Fallos no dejan recursos abiertos o estados inconsistentes.

**Validacion / prueba**

- Provocar 1-2 fallos controlados y revisar comportamiento.

### E04-04 - Integracion E2E de `trustpilot` en la pipeline

- `estado sugerido`: `TODO`

**Resultado esperado**

Trustpilot participa en jobs de scraping/analisis sin romper el baseline existente.

**Objetivo de la epica**

Definir criterio de done, resultados medidos y pendientes priorizados para la siguiente fase o iteracion.

**Dependencias**

- `E04-03`

**Riesgos a vigilar**

- Cerrar la fase sin criterios objetivos.
- No capturar deuda tecnica y repetir errores en la siguiente fase.

**Tickets**

#### T04-010 - Registrar source `trustpilot` en scrape-worker/orquestador

- `tipo`: `QA`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Correr la validacion final definida para la fase y capturar resultados, evidencias y problemas encontrados.

**Dependencias**

- `T04-002`
- `T04-006`
- `T04-008`

**Criterios de aceptacion**

1. Se ejecuta validacion final definida.
2. Resultados y evidencias quedan registrados.
3. Problemas se convierten en tickets o deuda.

**Validacion / prueba**

- Checklist de salida completada.

#### T04-011 - Crear smoke tests y fixtures de `trustpilot`

- `tipo`: `Documentacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Actualizar docs de fase/proyecto con lo implementado, limites conocidos y decisiones tomadas.

**Dependencias**

- `T04-010`

**Criterios de aceptacion**

1. Docs de fase y/o arquitectura actualizados.
2. Se listan limites y decisiones de diseÃ±o.
3. Se enlazan artefactos y pruebas relevantes.

**Validacion / prueba**

- Revision documental final.

#### T04-012 - Definir criterio de salida operativa de `trustpilot`

- `tipo`: `Planificacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Cerrar la fase dejando pendientes priorizados, con impacto y dependencias claras para la siguiente iteracion.

**Dependencias**

- `T04-010`
- `T04-011`

**Criterios de aceptacion**

1. Pendientes priorizados y categorizados.
2. Se identifican riesgos si se difieren.
3. La siguiente fase recibe inputs claros.

**Validacion / prueba**

- Revision del backlog de continuidad contra roadmap.
