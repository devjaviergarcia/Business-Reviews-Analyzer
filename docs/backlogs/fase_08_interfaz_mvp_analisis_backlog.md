# Backlog FASE_08 - Interfaz MVP de analisis

- `fase`: `fase_08`
- `documento de fase`: `docs/context/phases/fase_08_interfaz_mvp_analisis.md`
- `archivo backlog`: `docs/backlogs/fase_08_interfaz_mvp_analisis_backlog.md`
- `epicas`: `4`
- `tickets`: `12`
- `estado general sugerido`: `TODO`

## Objetivo operativo

crear una UI MVP para lanzar analisis, seguir progreso y consultar resultados/reportes

## Contexto y foco de trabajo

Esta fase ataca el siguiente objetivo del roadmap: **crear una UI MVP para lanzar analisis, seguir progreso y consultar resultados/reportes**.

Palabras clave de implementacion/seguimiento: `formulario, SSE, progreso, listados paginados, result view, demo`.

## Secuencia recomendada

1. Primero flujo principal: formulario -> job -> progreso -> resultado.
2. Reutilizar SSE y endpoints existentes.
3. Cerrar demo interna antes de refinamientos visuales mayores.

## Criterios de done de la fase

1. formulario de analisis funcional
2. progreso en tiempo real por SSE
3. vista de resultados/listados/reportes usable
4. smoke test UI y checklist de demo

## Resumen de epicas

| ID | Epica | Tickets | Resultado esperado |
| --- | --- | ---: | --- |
| `E08-01` | Formulario MVP y lanzamiento de analisis | 3 | La fase 'Interfaz MVP de analisis' tiene alcance, contratos y validaciones definidos. |
| `E08-02` | Seguimiento en tiempo real por SSE | 3 | Existe una implementacion funcional del bloque principal de la fase 'Interfaz MVP de analisis'. |
| `E08-03` | Visualizacion de resultados, listados y reportes | 3 | La fase puede operarse con visibilidad y diagnostico suficiente. |
| `E08-04` | Preparacion de demo y calidad minima del MVP | 3 | La fase 'Interfaz MVP de analisis' cierra con criterio objetivo y backlog de siguientes mejoras. |

## Epicas y tickets

### E08-01 - Formulario MVP y lanzamiento de analisis

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase 'Interfaz MVP de analisis' tiene alcance, contratos y validaciones definidos.

**Objetivo de la epica**

Bajar a contratos y decisiones tecnicas el objetivo de la fase: crear una UI MVP para lanzar analisis, seguir progreso y consultar resultados/reportes.

**Riesgos a vigilar**

- SobrediseÃ±ar antes de validar el flujo real.
- No definir validacion temprana y llegar al final con incertidumbre.

**Tickets**

#### T08-001 - DiseÃ±ar formulario de analisis (inputs basicos y opcionales)

- `tipo`: `Diseno`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Especificar que datos entran, que artefactos salen y como se valida el resultado de la fase. Contexto: formulario, SSE, progreso, listados paginados, result view, demo.

**Criterios de aceptacion**

1. Entradas y salidas definidas con ejemplos.
2. Precondiciones y dependencias explicitadas.
3. Criterio de validacion funcional acordado.

**Validacion / prueba**

- Revision tecnica del contrato contra el roadmap y el codigo actual.

#### T08-002 - Implementar envio a API y manejo de errores

- `tipo`: `QA`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Definir pruebas minimas para validar la fase sin esperar al final: smoke, fixtures, pruebas de integracion y chequeos manuales.

**Dependencias**

- `T08-001`

**Criterios de aceptacion**

1. Existe checklist de pruebas de la fase.
2. Cada prueba tiene comando/precondiciones.
3. Se definen criterios de exito/fallo.

**Validacion / prueba**

- Ejecucion de al menos una prueba de referencia o simulacion del flujo.

#### T08-003 - Persistir historial local de trabajos recientes

- `tipo`: `Documentacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Registrar los limites esperados de la fase y riesgos principales para Interfaz MVP de analisis, incluyendo deuda tecnica aceptada temporalmente.

**Dependencias**

- `T08-001`

**Criterios de aceptacion**

1. Lista de riesgos y mitigaciones iniciales.
2. Supuestos tecnicos explicitados.
3. Deuda tecnica temporal identificada.

**Validacion / prueba**

- Revision documental por consistencia con fase/contexto.

### E08-02 - Seguimiento en tiempo real por SSE

- `estado sugerido`: `TODO`

**Resultado esperado**

Existe una implementacion funcional del bloque principal de la fase 'Interfaz MVP de analisis'.

**Objetivo de la epica**

Construir el nucleo tecnico que materializa la fase: crear una UI MVP para lanzar analisis, seguir progreso y consultar resultados/reportes.

**Dependencias**

- `E08-01`

**Riesgos a vigilar**

- Acoplamiento excesivo con implementacion actual.
- Cambios internos rompiendo endpoints/contratos existentes.

**Tickets**

#### T08-004 - DiseÃ±ar timeline/estado de job

- `tipo`: `Implementacion`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Desarrollar la pieza central de la fase (modulos, servicios, conectores o UI) manteniendo separacion de responsabilidades. Pistas: formulario, SSE, progreso, listados paginados, result view, demo.

**Dependencias**

- `T08-001`

**Criterios de aceptacion**

1. La funcionalidad principal existe y se ejecuta.
2. Se respeta separacion de capas/modulos razonable.
3. El codigo sigue contratos definidos en la fase.

**Validacion / prueba**

- Smoke test del flujo principal de la fase.

#### T08-005 - Implementar consumo SSE y reconexion basica

- `tipo`: `Implementacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Exponer configuracion minima de timeouts, limites, modos o estrategias para poder operar y depurar sin tocar codigo continuamente.

**Dependencias**

- `T08-004`

**Criterios de aceptacion**

1. Parametros principales centralizados.
2. Defaults razonables definidos.
3. Se pueden sobreescribir para pruebas.

**Validacion / prueba**

- Probar ejecucion con defaults y con una variacion de parametros.

#### T08-006 - Traducir errores tecnicos a mensajes accionables

- `tipo`: `Integracion`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Conectar la nueva funcionalidad con servicios, jobs, endpoints o workers ya existentes sin romper compatibilidad externa.

**Dependencias**

- `T08-004`

**Criterios de aceptacion**

1. La integracion no rompe el baseline.
2. Estados/eventos/respuestas reflejan la nueva fase.
3. Se documenta el flujo de integracion.

**Validacion / prueba**

- E2E o integracion controlada con el sistema actual.

### E08-03 - Visualizacion de resultados, listados y reportes

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase puede operarse con visibilidad y diagnostico suficiente.

**Objetivo de la epica**

AÃ±adir logs, eventos, metricas y manejo de errores utiles para operar la fase y compararla con versiones futuras.

**Dependencias**

- `E08-02`

**Riesgos a vigilar**

- Falta de visibilidad real del comportamiento en runtime.
- Errores silenciosos o mensajes poco accionables.

**Tickets**

#### T08-007 - DiseÃ±ar vista de resultado de analisis

- `tipo`: `Observabilidad`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Emitir eventos y logs estructurados con etapa, conteos y errores para depuracion y seguimiento de progreso.

**Dependencias**

- `T08-006`

**Criterios de aceptacion**

1. Etapas clave emiten informacion de progreso.
2. Errores incluyen contexto suficiente.
3. Logs/eventos son consistentes con el resto del sistema.

**Validacion / prueba**

- Revisar logs/eventos en una ejecucion real de la fase.

#### T08-008 - Consumir endpoints paginados de business/reviews/analyses

- `tipo`: `Observabilidad`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Registrar metricas operativas (tiempo, volumen, exito/fallo, cobertura) para medir la fase y comparar iteraciones.

**Dependencias**

- `T08-007`

**Criterios de aceptacion**

1. Metricas minimas definidas y emitidas.
2. Se pueden consultar en logs/DB/eventos.
3. Sirven para detectar regresiones.

**Validacion / prueba**

- Ejecutar una corrida y revisar metricas registradas.

#### T08-009 - Integrar acceso/preview de informes

- `tipo`: `Bugfix/Implementacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Tipar errores frecuentes y definir que es recuperable vs no recuperable para mejorar estabilidad operativa.

**Dependencias**

- `T08-007`

**Criterios de aceptacion**

1. Errores comunes generan mensajes accionables.
2. Se aplican retries/fallbacks donde tenga sentido.
3. Fallos no dejan recursos abiertos o estados inconsistentes.

**Validacion / prueba**

- Provocar 1-2 fallos controlados y revisar comportamiento.

### E08-04 - Preparacion de demo y calidad minima del MVP

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase 'Interfaz MVP de analisis' cierra con criterio objetivo y backlog de siguientes mejoras.

**Objetivo de la epica**

Definir criterio de done, resultados medidos y pendientes priorizados para la siguiente fase o iteracion.

**Dependencias**

- `E08-03`

**Riesgos a vigilar**

- Cerrar la fase sin criterios objetivos.
- No capturar deuda tecnica y repetir errores en la siguiente fase.

**Tickets**

#### T08-010 - Documentar arranque frontend + API local/dev

- `tipo`: `QA`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Correr la validacion final definida para la fase y capturar resultados, evidencias y problemas encontrados.

**Dependencias**

- `T08-002`
- `T08-006`
- `T08-008`

**Criterios de aceptacion**

1. Se ejecuta validacion final definida.
2. Resultados y evidencias quedan registrados.
3. Problemas se convierten en tickets o deuda.

**Validacion / prueba**

- Checklist de salida completada.

#### T08-011 - AÃ±adir smoke test UI del flujo principal

- `tipo`: `Documentacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Actualizar docs de fase/proyecto con lo implementado, limites conocidos y decisiones tomadas.

**Dependencias**

- `T08-010`

**Criterios de aceptacion**

1. Docs de fase y/o arquitectura actualizados.
2. Se listan limites y decisiones de diseÃ±o.
3. Se enlazan artefactos y pruebas relevantes.

**Validacion / prueba**

- Revision documental final.

#### T08-012 - Crear checklist de demo interna del MVP

- `tipo`: `Planificacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Cerrar la fase dejando pendientes priorizados, con impacto y dependencias claras para la siguiente iteracion.

**Dependencias**

- `T08-010`
- `T08-011`

**Criterios de aceptacion**

1. Pendientes priorizados y categorizados.
2. Se identifican riesgos si se difieren.
3. La siguiente fase recibe inputs claros.

**Validacion / prueba**

- Revision del backlog de continuidad contra roadmap.
