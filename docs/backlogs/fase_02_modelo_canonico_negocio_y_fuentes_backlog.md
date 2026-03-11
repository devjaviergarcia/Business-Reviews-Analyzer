# Backlog FASE_02 - Modelo canonico de negocio y fuentes

- `fase`: `fase_02`
- `documento de fase`: `docs/context/phases/fase_02_modelo_canonico_negocio_y_fuentes.md`
- `archivo backlog`: `docs/backlogs/fase_02_modelo_canonico_negocio_y_fuentes_backlog.md`
- `epicas`: `4`
- `tickets`: `12`
- `estado general sugerido`: `TODO`

## Objetivo operativo

centralizar negocio, perfiles de fuente y menciones/reviews en un modelo canonico multi-fuente

## Contexto y foco de trabajo

Esta fase ataca el siguiente objetivo del roadmap: **centralizar negocio, perfiles de fuente y menciones/reviews en un modelo canonico multi-fuente**.

Palabras clave de implementacion/seguimiento: `business, source_profile, mention, review, matching, raw_payload, ciudad, pais, idioma`.

## Secuencia recomendada

1. Modelar primero, migrar Google Maps despues.
2. Guardar siempre raw_payload.
3. Mantener compatibilidad de endpoints con DTOs/adaptadores.

## Criterios de done de la fase

1. Business y SourceProfile definidos y persistidos
2. Mention/Review canonicos con dedupe basico
3. matching reusable por conectores
4. consultas/API desacopladas del modelo legacy

## Resumen de epicas

| ID | Epica | Tickets | Resultado esperado |
| --- | --- | ---: | --- |
| `E02-01` | Modelo `Business` y `SourceProfile` | 3 | La fase 'Modelo canonico de negocio y fuentes' tiene alcance, contratos y validaciones definidos. |
| `E02-02` | Modelo canonico de `Mention`/`Review` | 3 | Existe una implementacion funcional del bloque principal de la fase 'Modelo canonico de negocio y fuentes'. |
| `E02-03` | Entity matching reusable y explicable | 3 | La fase puede operarse con visibilidad y diagnostico suficiente. |
| `E02-04` | Consultas/API sobre el modelo canonico | 3 | La fase 'Modelo canonico de negocio y fuentes' cierra con criterio objetivo y backlog de siguientes mejoras. |

## Epicas y tickets

### E02-01 - Modelo `Business` y `SourceProfile`

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase 'Modelo canonico de negocio y fuentes' tiene alcance, contratos y validaciones definidos.

**Objetivo de la epica**

Bajar a contratos y decisiones tecnicas el objetivo de la fase: centralizar negocio, perfiles de fuente y menciones/reviews en un modelo canonico multi-fuente.

**Riesgos a vigilar**

- SobrediseÃ±ar antes de validar el flujo real.
- No definir validacion temprana y llegar al final con incertidumbre.

**Tickets**

#### T02-001 - DiseÃ±ar entidad `Business` enriquecida

- `tipo`: `Diseno`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Especificar que datos entran, que artefactos salen y como se valida el resultado de la fase. Contexto: business, source_profile, mention, review, matching, raw_payload, ciudad, pais, idioma.

**Criterios de aceptacion**

1. Entradas y salidas definidas con ejemplos.
2. Precondiciones y dependencias explicitadas.
3. Criterio de validacion funcional acordado.

**Validacion / prueba**

- Revision tecnica del contrato contra el roadmap y el codigo actual.

#### T02-002 - DiseÃ±ar entidad `SourceProfile` por negocio/fuente

- `tipo`: `QA`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Definir pruebas minimas para validar la fase sin esperar al final: smoke, fixtures, pruebas de integracion y chequeos manuales.

**Dependencias**

- `T02-001`

**Criterios de aceptacion**

1. Existe checklist de pruebas de la fase.
2. Cada prueba tiene comando/precondiciones.
3. Se definen criterios de exito/fallo.

**Validacion / prueba**

- Ejecucion de al menos una prueba de referencia o simulacion del flujo.

#### T02-003 - Implementar repositorios e indices de negocio/fuentes

- `tipo`: `Documentacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Registrar los limites esperados de la fase y riesgos principales para Modelo canonico de negocio y fuentes, incluyendo deuda tecnica aceptada temporalmente.

**Dependencias**

- `T02-001`

**Criterios de aceptacion**

1. Lista de riesgos y mitigaciones iniciales.
2. Supuestos tecnicos explicitados.
3. Deuda tecnica temporal identificada.

**Validacion / prueba**

- Revision documental por consistencia con fase/contexto.

### E02-02 - Modelo canonico de `Mention`/`Review`

- `estado sugerido`: `TODO`

**Resultado esperado**

Existe una implementacion funcional del bloque principal de la fase 'Modelo canonico de negocio y fuentes'.

**Objetivo de la epica**

Construir el nucleo tecnico que materializa la fase: centralizar negocio, perfiles de fuente y menciones/reviews en un modelo canonico multi-fuente.

**Dependencias**

- `E02-01`

**Riesgos a vigilar**

- Acoplamiento excesivo con implementacion actual.
- Cambios internos rompiendo endpoints/contratos existentes.

**Tickets**

#### T02-004 - DiseÃ±ar `Mention` y `ReviewMention`

- `tipo`: `Implementacion`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Desarrollar la pieza central de la fase (modulos, servicios, conectores o UI) manteniendo separacion de responsabilidades. Pistas: business, source_profile, mention, review, matching, raw_payload, ciudad, pais, idioma.

**Dependencias**

- `T02-001`

**Criterios de aceptacion**

1. La funcionalidad principal existe y se ejecuta.
2. Se respeta separacion de capas/modulos razonable.
3. El codigo sigue contratos definidos en la fase.

**Validacion / prueba**

- Smoke test del flujo principal de la fase.

#### T02-005 - Implementar repositorio canonico con dedupe basico

- `tipo`: `Implementacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Exponer configuracion minima de timeouts, limites, modos o estrategias para poder operar y depurar sin tocar codigo continuamente.

**Dependencias**

- `T02-004`

**Criterios de aceptacion**

1. Parametros principales centralizados.
2. Defaults razonables definidos.
3. Se pueden sobreescribir para pruebas.

**Validacion / prueba**

- Probar ejecucion con defaults y con una variacion de parametros.

#### T02-006 - Migrar Google Maps al modelo canonico

- `tipo`: `Integracion`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Conectar la nueva funcionalidad con servicios, jobs, endpoints o workers ya existentes sin romper compatibilidad externa.

**Dependencias**

- `T02-004`

**Criterios de aceptacion**

1. La integracion no rompe el baseline.
2. Estados/eventos/respuestas reflejan la nueva fase.
3. Se documenta el flujo de integracion.

**Validacion / prueba**

- E2E o integracion controlada con el sistema actual.

### E02-03 - Entity matching reusable y explicable

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase puede operarse con visibilidad y diagnostico suficiente.

**Objetivo de la epica**

AÃ±adir logs, eventos, metricas y manejo de errores utiles para operar la fase y compararla con versiones futuras.

**Dependencias**

- `E02-02`

**Riesgos a vigilar**

- Falta de visibilidad real del comportamiento en runtime.
- Errores silenciosos o mensajes poco accionables.

**Tickets**

#### T02-007 - Definir scoring y umbrales de matching

- `tipo`: `Observabilidad`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Emitir eventos y logs estructurados con etapa, conteos y errores para depuracion y seguimiento de progreso.

**Dependencias**

- `T02-006`

**Criterios de aceptacion**

1. Etapas clave emiten informacion de progreso.
2. Errores incluyen contexto suficiente.
3. Logs/eventos son consistentes con el resto del sistema.

**Validacion / prueba**

- Revisar logs/eventos en una ejecucion real de la fase.

#### T02-008 - Implementar servicio de matching central

- `tipo`: `Observabilidad`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Registrar metricas operativas (tiempo, volumen, exito/fallo, cobertura) para medir la fase y comparar iteraciones.

**Dependencias**

- `T02-007`

**Criterios de aceptacion**

1. Metricas minimas definidas y emitidas.
2. Se pueden consultar en logs/DB/eventos.
3. Sirven para detectar regresiones.

**Validacion / prueba**

- Ejecutar una corrida y revisar metricas registradas.

#### T02-009 - DiseÃ±ar flujo de candidatos ambiguos/manual review

- `tipo`: `Bugfix/Implementacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Tipar errores frecuentes y definir que es recuperable vs no recuperable para mejorar estabilidad operativa.

**Dependencias**

- `T02-007`

**Criterios de aceptacion**

1. Errores comunes generan mensajes accionables.
2. Se aplican retries/fallbacks donde tenga sentido.
3. Fallos no dejan recursos abiertos o estados inconsistentes.

**Validacion / prueba**

- Provocar 1-2 fallos controlados y revisar comportamiento.

### E02-04 - Consultas/API sobre el modelo canonico

- `estado sugerido`: `TODO`

**Resultado esperado**

La fase 'Modelo canonico de negocio y fuentes' cierra con criterio objetivo y backlog de siguientes mejoras.

**Objetivo de la epica**

Definir criterio de done, resultados medidos y pendientes priorizados para la siguiente fase o iteracion.

**Dependencias**

- `E02-03`

**Riesgos a vigilar**

- Cerrar la fase sin criterios objetivos.
- No capturar deuda tecnica y repetir errores en la siguiente fase.

**Tickets**

#### T02-010 - Crear query services para negocio/fuentes/menciones

- `tipo`: `QA`
- `prioridad`: `Alta`
- `estado sugerido`: `TODO`

**Descripcion**

Correr la validacion final definida para la fase y capturar resultados, evidencias y problemas encontrados.

**Dependencias**

- `T02-002`
- `T02-006`
- `T02-008`

**Criterios de aceptacion**

1. Se ejecuta validacion final definida.
2. Resultados y evidencias quedan registrados.
3. Problemas se convierten en tickets o deuda.

**Validacion / prueba**

- Checklist de salida completada.

#### T02-011 - AÃ±adir endpoints de SourceProfile/estado de sync

- `tipo`: `Documentacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Actualizar docs de fase/proyecto con lo implementado, limites conocidos y decisiones tomadas.

**Dependencias**

- `T02-010`

**Criterios de aceptacion**

1. Docs de fase y/o arquitectura actualizados.
2. Se listan limites y decisiones de diseÃ±o.
3. Se enlazan artefactos y pruebas relevantes.

**Validacion / prueba**

- Revision documental final.

#### T02-012 - Plan de deprecacion de estructuras legacy

- `tipo`: `Planificacion`
- `prioridad`: `Media`
- `estado sugerido`: `TODO`

**Descripcion**

Cerrar la fase dejando pendientes priorizados, con impacto y dependencias claras para la siguiente iteracion.

**Dependencias**

- `T02-010`
- `T02-011`

**Criterios de aceptacion**

1. Pendientes priorizados y categorizados.
2. Se identifican riesgos si se difieren.
3. La siguiente fase recibe inputs claros.

**Validacion / prueba**

- Revision del backlog de continuidad contra roadmap.
