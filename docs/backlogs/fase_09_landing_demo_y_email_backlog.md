# Backlog FASE_09 - Landing, demo y email

- `fase`: `fase_09`
- `documento de fase`: `docs/context/phases/fase_09_landing_demo_y_email.md`
- `archivo backlog`: `docs/backlogs/fase_09_landing_demo_y_email_backlog.md`
- `epicas`: `4`
- `tickets`: `12`
- `estado general sugerido`: `TODO`

## Objetivo operativo

captar leads con landing y enviar mini-analisis por email para agendar demo

## Contexto y foco de trabajo

Esta fase ataca el siguiente objetivo del roadmap: **captar leads con landing y enviar mini-analisis por email para agendar demo**.

Palabras clave de implementacion/seguimiento: `landing, lead capture, mini-analisis, email, funnel, compliance`.

## Secuencia recomendada

1. Separar captacion, procesamiento y envio.
2. Definir el producto mini-analisis antes de implementarlo.
3. AÃ±adir trazabilidad de lead/job/email desde el inicio.

## Criterios de done de la fase

1. landing y formulario funcionales
2. captura de leads + job mini-analisis + email operativo
3. metricas basicas del funnel disponibles
4. validaciones/anti-abuso y checklist compliance minima

## Resumen de epicas

| ID | Epica | Tickets | Resultado esperado |
| --- | --- | ---: | --- |
| `E09-01` | Producto de captacion y mini-analisis | 3 | La fase 'Landing, demo y email' tiene alcance, contratos y validaciones definidos. |
| `E09-02` | Landing + captura de leads + persistencia | 3 | Existe una implementacion funcional del bloque principal de la fase 'Landing, demo y email'. |
| `E09-03` | Mini-analisis asincrono y envio de email | 3 | La fase puede operarse con visibilidad y diagnostico suficiente. |
| `E09-04` | Operacion del funnel, anti-abuso y compliance | 3 | La fase 'Landing, demo y email' cierra con criterio objetivo y backlog de siguientes mejoras. |

## Epicas y tickets

### E09-01 - Producto de captacion y mini-analisis

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase 'Landing, demo y email' tiene alcance, contratos y validaciones definidos.

**Objetivo de la epica**

Bajar a contratos y decisiones tecnicas el objetivo de la fase: captar leads con landing y enviar mini-analisis por email para agendar demo.

**Riesgos a vigilar**

- SobrediseÃ±ar antes de validar el flujo real.
- No definir validacion temprana y llegar al final con incertidumbre.

**Tickets**

#### T09-001 - DiseÃ±ar estructura/copy de la landing

- `tipo`: `Diseno`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Especificar que datos entran, que artefactos salen y como se valida el resultado de la fase. Contexto: landing, lead capture, mini-analisis, email, funnel, compliance.

**Criterios de aceptacion**

1. Entradas y salidas definidas con ejemplos.
2. Precondiciones y dependencias explicitadas.
3. Criterio de validacion funcional acordado.

**Validacion / prueba**

- Revision tecnica del contrato contra el roadmap y el codigo actual.

#### T09-002 - Definir inputs minimos del formulario de lead

- `tipo`: `QA`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Definir pruebas minimas para validar la fase sin esperar al final: smoke, fixtures, pruebas de integracion y chequeos manuales.

**Dependencias**

- `T09-001`

**Criterios de aceptacion**

1. Existe checklist de pruebas de la fase.
2. Cada prueba tiene comando/precondiciones.
3. Se definen criterios de exito/fallo.

**Validacion / prueba**

- Ejecucion de al menos una prueba de referencia o simulacion del flujo.

#### T09-003 - Definir formato del mini-analisis por email

- `tipo`: `Documentacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Registrar los limites esperados de la fase y riesgos principales para Landing, demo y email, incluyendo deuda tecnica aceptada temporalmente.

**Dependencias**

- `T09-001`

**Criterios de aceptacion**

1. Lista de riesgos y mitigaciones iniciales.
2. Supuestos tecnicos explicitados.
3. Deuda tecnica temporal identificada.

**Validacion / prueba**

- Revision documental por consistencia con fase/contexto.

### E09-02 - Landing + captura de leads + persistencia

- `estado sugerido`: `TODO`

**Resultado esperado**

Existe una implementacion funcional del bloque principal de la fase 'Landing, demo y email'.

**Objetivo de la epica**

Construir el nucleo tecnico que materializa la fase: captar leads con landing y enviar mini-analisis por email para agendar demo.

**Dependencias**

- `E09-01`

**Riesgos a vigilar**

- Acoplamiento excesivo con implementacion actual.
- Cambios internos rompiendo endpoints/contratos existentes.

**Tickets**

#### T09-004 - Implementar landing y formulario MVP

- `tipo`: `Implementacion`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Desarrollar la pieza central de la fase (modulos, servicios, conectores o UI) manteniendo separacion de responsabilidades. Pistas: landing, lead capture, mini-analisis, email, funnel, compliance.

**Dependencias**

- `T09-001`

**Criterios de aceptacion**

1. La funcionalidad principal existe y se ejecuta.
2. Se respeta separacion de capas/modulos razonable.
3. El codigo sigue contratos definidos en la fase.

**Validacion / prueba**

- Smoke test del flujo principal de la fase.

#### T09-005 - Crear endpoint de captura de lead y solicitud

- `tipo`: `Implementacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Exponer configuracion minima de timeouts, limites, modos o estrategias para poder operar y depurar sin tocar codigo continuamente.

**Dependencias**

- `T09-004`

**Criterios de aceptacion**

1. Parametros principales centralizados.
2. Defaults razonables definidos.
3. Se pueden sobreescribir para pruebas.

**Validacion / prueba**

- Probar ejecucion con defaults y con una variacion de parametros.

#### T09-006 - Modelar persistencia de leads y estados

- `tipo`: `Integracion`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Conectar la nueva funcionalidad con servicios, jobs, endpoints o workers ya existentes sin romper compatibilidad externa.

**Dependencias**

- `T09-004`

**Criterios de aceptacion**

1. La integracion no rompe el baseline.
2. Estados/eventos/respuestas reflejan la nueva fase.
3. Se documenta el flujo de integracion.

**Validacion / prueba**

- E2E o integracion controlada con el sistema actual.

### E09-03 - Mini-analisis asincrono y envio de email

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase puede operarse con visibilidad y diagnostico suficiente.

**Objetivo de la epica**

AÃ±adir logs, eventos, metricas y manejo de errores utiles para operar la fase y compararla con versiones futuras.

**Dependencias**

- `E09-02`

**Riesgos a vigilar**

- Falta de visibilidad real del comportamiento en runtime.
- Errores silenciosos o mensajes poco accionables.

**Tickets**

#### T09-007 - Definir flujo de mini-analisis (job) y limites

- `tipo`: `Observabilidad`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Emitir eventos y logs estructurados con etapa, conteos y errores para depuracion y seguimiento de progreso.

**Dependencias**

- `T09-006`

**Criterios de aceptacion**

1. Etapas clave emiten informacion de progreso.
2. Errores incluyen contexto suficiente.
3. Logs/eventos son consistentes con el resto del sistema.

**Validacion / prueba**

- Revisar logs/eventos en una ejecucion real de la fase.

#### T09-008 - Implementar plantilla de email y servicio de envio

- `tipo`: `Observabilidad`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Registrar metricas operativas (tiempo, volumen, exito/fallo, cobertura) para medir la fase y comparar iteraciones.

**Dependencias**

- `T09-007`

**Criterios de aceptacion**

1. Metricas minimas definidas y emitidas.
2. Se pueden consultar en logs/DB/eventos.
3. Sirven para detectar regresiones.

**Validacion / prueba**

- Ejecutar una corrida y revisar metricas registradas.

#### T09-009 - Integrar workers/pipeline en flujo de mini-analisis

- `tipo`: `Bugfix/Implementacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Tipar errores frecuentes y definir que es recuperable vs no recuperable para mejorar estabilidad operativa.

**Dependencias**

- `T09-007`

**Criterios de aceptacion**

1. Errores comunes generan mensajes accionables.
2. Se aplican retries/fallbacks donde tenga sentido.
3. Fallos no dejan recursos abiertos o estados inconsistentes.

**Validacion / prueba**

- Provocar 1-2 fallos controlados y revisar comportamiento.

### E09-04 - Operacion del funnel, anti-abuso y compliance

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase 'Landing, demo y email' cierra con criterio objetivo y backlog de siguientes mejoras.

**Objetivo de la epica**

Definir criterio de done, resultados medidos y pendientes priorizados para la siguiente fase o iteracion.

**Dependencias**

- `E09-03`

**Riesgos a vigilar**

- Cerrar la fase sin criterios objetivos.
- No capturar deuda tecnica y repetir errores en la siguiente fase.

**Tickets**

#### T09-010 - Registrar metricas del funnel y seguimiento

- `tipo`: `QA`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Correr la validacion final definida para la fase y capturar resultados, evidencias y problemas encontrados.

**Dependencias**

- `T09-002`
- `T09-006`
- `T09-008`

**Criterios de aceptacion**

1. Se ejecuta validacion final definida.
2. Resultados y evidencias quedan registrados.
3. Problemas se convierten en tickets o deuda.

**Validacion / prueba**

- Checklist de salida completada.

#### T09-011 - AÃ±adir validaciones y limites anti-abuso

- `tipo`: `Documentacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Actualizar docs de fase/proyecto con lo implementado, limites conocidos y decisiones tomadas.

**Dependencias**

- `T09-010`

**Criterios de aceptacion**

1. Docs de fase y/o arquitectura actualizados.
2. Se listan limites y decisiones de diseÃ±o.
3. Se enlazan artefactos y pruebas relevantes.

**Validacion / prueba**

- Revision documental final.

#### T09-012 - Checklist legal/compliance minima

- `tipo`: `Planificacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Cerrar la fase dejando pendientes priorizados, con impacto y dependencias claras para la siguiente iteracion.

**Dependencias**

- `T09-010`
- `T09-011`

**Criterios de aceptacion**

1. Pendientes priorizados y categorizados.
2. Se identifican riesgos si se difieren.
3. La siguiente fase recibe inputs claros.

**Validacion / prueba**

- Revision del backlog de continuidad contra roadmap.
