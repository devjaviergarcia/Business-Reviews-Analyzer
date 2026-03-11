# Backlog FASE_01 - Arquitectura modular y workers

- `fase`: `fase_01`
- `documento de fase`: `docs/context/phases/fase_01_arquitectura_modular_y_workers.md`
- `archivo backlog`: `docs/backlogs/fase_01_arquitectura_modular_y_workers_backlog.md`
- `epicas`: `4`
- `tickets`: `12`
- `estado general sugerido`: `TODO`

## Objetivo operativo

separar scraping, analisis y reporte en workers con contratos de mensajes listos para RabbitMQ

## Contexto y foco de trabajo

Esta fase ataca el siguiente objetivo del roadmap: **separar scraping, analisis y reporte en workers con contratos de mensajes listos para RabbitMQ**.

Palabras clave de implementacion/seguimiento: `scrape-worker, analysis-worker, report-worker, orchestrator, payloads, broker abstraction`.

## Secuencia recomendada

1. Definir contratos antes de mover codigo.
2. Mantener endpoints actuales como fachada estable.
3. Migracion a RabbitMQ incremental, no de golpe.

## Criterios de done de la fase

1. contratos de mensajes y estados de job definidos
2. servicios/casos de uso separados con API compatible
3. workers especializados ejecutables
4. plan de migracion a RabbitMQ definido

## Resumen de epicas

| ID | Epica | Tickets | Resultado esperado |
| --- | --- | ---: | --- |
| `E01-01` | Contratos de mensajes y estados de job por etapa | 3 | La fase 'Arquitectura modular y workers' tiene alcance, contratos y validaciones definidos. |
| `E01-02` | Refactor de orquestacion y puertos/adaptadores | 3 | Existe una implementacion funcional del bloque principal de la fase 'Arquitectura modular y workers'. |
| `E01-03` | Workers especializados y colas logicas | 3 | La fase puede operarse con visibilidad y diagnostico suficiente. |
| `E01-04` | Preparacion e integracion gradual con RabbitMQ | 3 | La fase 'Arquitectura modular y workers' cierra con criterio objetivo y backlog de siguientes mejoras. |

## Epicas y tickets

### E01-01 - Contratos de mensajes y estados de job por etapa

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase 'Arquitectura modular y workers' tiene alcance, contratos y validaciones definidos.

**Objetivo de la epica**

Definir payloads y estados de `scrape-worker`, `analysis-worker` y `report-worker`, base de la orquestacion y RabbitMQ.

**Riesgos a vigilar**

- SobrediseÃ±ar antes de validar el flujo real.
- No definir validacion temprana y llegar al final con incertidumbre.

**Tickets**

#### T01-001 - Definir contrato de entrada/salida de la fase

- `tipo`: `Diseno`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Especificar que datos entran, que artefactos salen y como se valida el resultado de la fase. Contexto: scrape-worker, analysis-worker, report-worker, orchestrator, payloads, broker abstraction.

**Criterios de aceptacion**

1. Entradas y salidas definidas con ejemplos.
2. Precondiciones y dependencias explicitadas.
3. Criterio de validacion funcional acordado.

**Validacion / prueba**

- Revision tecnica del contrato contra el roadmap y el codigo actual.

#### T01-002 - DiseÃ±ar plan de pruebas/smoke de la fase

- `tipo`: `QA`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Definir pruebas minimas para validar la fase sin esperar al final: smoke, fixtures, pruebas de integracion y chequeos manuales.

**Dependencias**

- `T01-001`

**Criterios de aceptacion**

1. Existe checklist de pruebas de la fase.
2. Cada prueba tiene comando/precondiciones.
3. Se definen criterios de exito/fallo.

**Validacion / prueba**

- Ejecucion de al menos una prueba de referencia o simulacion del flujo.

#### T01-003 - Documentar supuestos, limites y riesgos

- `tipo`: `Documentacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Registrar los limites esperados de la fase y riesgos principales para Arquitectura modular y workers, incluyendo deuda tecnica aceptada temporalmente.

**Dependencias**

- `T01-001`

**Criterios de aceptacion**

1. Lista de riesgos y mitigaciones iniciales.
2. Supuestos tecnicos explicitados.
3. Deuda tecnica temporal identificada.

**Validacion / prueba**

- Revision documental por consistencia con fase/contexto.

### E01-02 - Refactor de orquestacion y puertos/adaptadores

- `estado sugerido`: `TODO`

**Resultado esperado**

Existe una implementacion funcional del bloque principal de la fase 'Arquitectura modular y workers'.

**Objetivo de la epica**

Construir el nucleo tecnico que materializa la fase: separar scraping, analisis y reporte en workers con contratos de mensajes listos para RabbitMQ.

**Dependencias**

- `E01-01`

**Riesgos a vigilar**

- Acoplamiento excesivo con implementacion actual.
- Cambios internos rompiendo endpoints/contratos existentes.

**Tickets**

#### T01-004 - Separar query services, orchestration y analyze use cases

- `tipo`: `Implementacion`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Desarrollar la pieza central de la fase (modulos, servicios, conectores o UI) manteniendo separacion de responsabilidades. Pistas: scrape-worker, analysis-worker, report-worker, orchestrator, payloads, broker abstraction.

**Dependencias**

- `T01-001`

**Criterios de aceptacion**

1. La funcionalidad principal existe y se ejecuta.
2. Se respeta separacion de capas/modulos razonable.
3. El codigo sigue contratos definidos en la fase.

**Validacion / prueba**

- Smoke test del flujo principal de la fase.

#### T01-005 - Introducir puertos para scraper y proveedor LLM

- `tipo`: `Implementacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Exponer configuracion minima de timeouts, limites, modos o estrategias para poder operar y depurar sin tocar codigo continuamente.

**Dependencias**

- `T01-004`

**Criterios de aceptacion**

1. Parametros principales centralizados.
2. Defaults razonables definidos.
3. Se pueden sobreescribir para pruebas.

**Validacion / prueba**

- Probar ejecucion con defaults y con una variacion de parametros.

#### T01-006 - Integrar la fase con la pipeline/API existente

- `tipo`: `Integracion`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Conectar la nueva funcionalidad con servicios, jobs, endpoints o workers ya existentes sin romper compatibilidad externa.

**Dependencias**

- `T01-004`

**Criterios de aceptacion**

1. La integracion no rompe el baseline.
2. Estados/eventos/respuestas reflejan la nueva fase.
3. Se documenta el flujo de integracion.

**Validacion / prueba**

- E2E o integracion controlada con el sistema actual.

### E01-03 - Workers especializados y colas logicas

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase puede operarse con visibilidad y diagnostico suficiente.

**Objetivo de la epica**

AÃ±adir logs, eventos, metricas y manejo de errores utiles para operar la fase y compararla con versiones futuras.

**Dependencias**

- `E01-02`

**Riesgos a vigilar**

- Falta de visibilidad real del comportamiento en runtime.
- Errores silenciosos o mensajes poco accionables.

**Tickets**

#### T01-007 - Crear entrypoints `scrape-worker`, `analysis-worker`, `report-worker`

- `tipo`: `Observabilidad`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Emitir eventos y logs estructurados con etapa, conteos y errores para depuracion y seguimiento de progreso.

**Dependencias**

- `T01-006`

**Criterios de aceptacion**

1. Etapas clave emiten informacion de progreso.
2. Errores incluyen contexto suficiente.
3. Logs/eventos son consistentes con el resto del sistema.

**Validacion / prueba**

- Revisar logs/eventos en una ejecucion real de la fase.

#### T01-008 - Introducir `queue_name`/`routing_key` internos

- `tipo`: `Observabilidad`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Registrar metricas operativas (tiempo, volumen, exito/fallo, cobertura) para medir la fase y comparar iteraciones.

**Dependencias**

- `T01-007`

**Criterios de aceptacion**

1. Metricas minimas definidas y emitidas.
2. Se pueden consultar en logs/DB/eventos.
3. Sirven para detectar regresiones.

**Validacion / prueba**

- Ejecutar una corrida y revisar metricas registradas.

#### T01-009 - Configurar concurrencia y timeouts por worker

- `tipo`: `Bugfix/Implementacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Tipar errores frecuentes y definir que es recuperable vs no recuperable para mejorar estabilidad operativa.

**Dependencias**

- `T01-007`

**Criterios de aceptacion**

1. Errores comunes generan mensajes accionables.
2. Se aplican retries/fallbacks donde tenga sentido.
3. Fallos no dejan recursos abiertos o estados inconsistentes.

**Validacion / prueba**

- Provocar 1-2 fallos controlados y revisar comportamiento.

### E01-04 - Preparacion e integracion gradual con RabbitMQ

- `estado sugerido`: `DEFERRED`

**Resultado esperado**

La fase 'Arquitectura modular y workers' cierra con criterio objetivo y backlog de siguientes mejoras.

**Objetivo de la epica**

Definir criterio de done, resultados medidos y pendientes priorizados para la siguiente fase o iteracion.

**Dependencias**

- `E01-03`

**Riesgos a vigilar**

- Cerrar la fase sin criterios objetivos.
- No capturar deuda tecnica y repetir errores en la siguiente fase.

**Tickets**

#### T01-010 - DiseÃ±ar topologia RabbitMQ (exchanges/queues/DLQ)

- `tipo`: `QA`
- `prioridad`: `Alta`
- `estado sugerido`: `DEFERRED`

**Descripcion**

Correr la validacion final definida para la fase y capturar resultados, evidencias y problemas encontrados.

**Dependencias**

- `T01-002`
- `T01-006`
- `T01-008`

**Criterios de aceptacion**

1. Se ejecuta validacion final definida.
2. Resultados y evidencias quedan registrados.
3. Problemas se convierten en tickets o deuda.

**Validacion / prueba**

- Checklist de salida completada.

#### T01-011 - Crear abstraccion de broker y adaptadores

- `tipo`: `Documentacion`
- `prioridad`: `Media`
- `estado sugerido`: `DEFERRED`

**Descripcion**

Actualizar docs de fase/proyecto con lo implementado, limites conocidos y decisiones tomadas.

**Dependencias**

- `T01-010`

**Criterios de aceptacion**

1. Docs de fase y/o arquitectura actualizados.
2. Se listan limites y decisiones de diseÃ±o.
3. Se enlazan artefactos y pruebas relevantes.

**Validacion / prueba**

- Revision documental final.

#### T01-012 - Plan de migracion incremental con rollback

- `tipo`: `Planificacion`
- `prioridad`: `Media`
- `estado sugerido`: `DEFERRED`

**Descripcion**

Cerrar la fase dejando pendientes priorizados, con impacto y dependencias claras para la siguiente iteracion.

**Dependencias**

- `T01-010`
- `T01-011`

**Criterios de aceptacion**

1. Pendientes priorizados y categorizados.
2. Se identifican riesgos si se difieren.
3. La siguiente fase recibe inputs claros.

**Validacion / prueba**

- Revision del backlog de continuidad contra roadmap.

