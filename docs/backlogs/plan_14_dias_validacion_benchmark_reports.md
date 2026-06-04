# Plan 14 Dias: Estudio Local, Lead Report y Paid Report

## Objetivo

Validar en 14 dias si Repiq puede conseguir demanda real para un producto recurrente de reputacion local.

La validacion no consiste en terminar un CRM grande. Consiste en producir:

1. Un estudio publico compartible sobre un nicho local.
2. Un flujo de captacion opt-in para pedir un informe.
3. Un lead report gratuito suficientemente bueno para activar interes.
4. Un paid report mensual claramente mas valioso que el informe gratuito.
5. Tracking de origen por canal, CTA y campana.

## Hipotesis

Un negocio local pagaria una suscripcion mensual si recibe un radar accionable que le compara con competidores cercanos y le dice que mejorar cada mes para captar mas clientes desde Google Maps, Tripadvisor y otros canales de reputacion.

## Nicho Inicial

- Sector: restaurantes.
- Ciudad: Cordoba.
- Tamano del benchmark: 100 negocios.
- Oferta beta: 79 EUR/mes.
- Resultado esperado en 14 dias:
  - 100 negocios analizados.
  - 1 estudio publico publicado.
  - 20 lead reports generados.
  - 10 solicitudes de informe.
  - 1-3 pagos o precompromisos.

## Principios De Producto

- No construir mas CRM avanzado hasta validar senal comercial.
- No enviar email comercial frio.
- Todo envio nace de opt-in o solicitud explicita de informe.
- El informe gratuito vende claridad, no profundidad total.
- El informe pagado vende seguimiento, comparativa y accion.
- Cada lead debe tener origen trackeable desde el primer click.

## Cambios Por Parte De La Aplicacion

### 1. Discovery y Benchmark

Crear una capa de benchmark por ciudad/sector que reutilice el discovery actual.

Cambios:

- Nueva entidad `benchmark_runs`.
- Nueva entidad `benchmark_businesses` o extension controlada de `crm_leads` con `benchmark_id`.
- Nueva accion UI: "Crear estudio local".
- Nuevo flujo backend: `benchmark_run -> discovery -> enrich listings -> snapshot`.
- Guardar los negocios aunque no tengan email.

Campos minimos por negocio:

- `business_name`
- `category`
- `city`
- `address`
- `maps_url`
- `discovery_rank`
- `rating`
- `review_count`
- `phone`
- `website`
- `source`
- `benchmark_id`
- `opportunity_score`
- `reputation_score`
- `visibility_score`
- `conversion_risk_score`
- `listing_enriched`

Criterios de aceptacion:

- Se puede lanzar un benchmark de `restaurantes Cordoba`.
- El sistema obtiene hasta 100 negocios o marca claramente el motivo si no llega.
- Cada negocio queda asociado al `benchmark_run_id`.
- La pantalla CRM permite filtrar negocios por benchmark.
- El benchmark no requiere email ni consentimiento, porque no envia comunicaciones.

### 2. Analisis De Competidores

Anadir un modulo para seleccionar competidores dado un negocio.

Cambios:

- Nueva funcion `select_competitors_for_business(business, candidates)`.
- Nueva entidad `competitor_sets`.
- Nuevo hook en el pipeline: despues de enriquecer el listing, seleccionar competidores.
- Nueva seccion en reportes: "Comparativa local".

Reglas V1:

- Misma ciudad.
- Categoria igual o similar.
- Rating igual o superior cuando sea posible.
- Volumen de reviews comparable o superior.
- Priorizar negocios con mejor posicion aparente en discovery.
- Seleccionar 5 competidores:
  - 2 lideres locales.
  - 2 similares.
  - 1 aspiracional/top.

Output esperado:

- `competitor_name`
- `maps_url`
- `rating`
- `review_count`
- `website`
- `category`
- `distance_hint` si existe.
- `why_selected`
- `relative_position`

Criterios de aceptacion:

- Dado un negocio del benchmark, el sistema devuelve 3-5 competidores.
- Si no hay suficientes competidores, el report lo muestra sin contenedores vacios.
- La seleccion es determinista para el mismo dataset.

### 3. Hook De Estudio Profundo

Crear un hook de analisis profundo que pueda usarse tanto por benchmark agregado como por paid report.

Cambios:

- Nueva funcion `build_deep_study_snapshot(...)`.
- Input: negocio, listing, reviews, competidores, benchmark local.
- Output estructurado:
  - `executive_summary`
  - `strengths`
  - `risks`
  - `recurring_topics`
  - `competitor_gaps`
  - `monthly_actions`
  - `response_templates`
  - `score_breakdown`

Uso:

- Lead report usa una version reducida.
- Paid report usa la version completa.
- Estudio publico usa agregados anonimizados.

Criterios de aceptacion:

- El hook no depende del HTML.
- El hook devuelve JSON validable.
- Si faltan reviews, el analisis se degrada a listing + benchmark.
- Se registran warnings de datos incompletos.

### 4. Lead Report Gratuito

Modificar el lead report para que sea corto, claro y orientado a conversion.

Contenido V1:

- Score general.
- Posicion frente a la media local.
- 3 oportunidades principales.
- 1 comparativa ligera con competidores.
- 1 accion recomendada inmediata.
- CTA al paid report mensual.

Cambios:

- Nuevo template `lead_report`.
- Nueva entidad `lead_reports`.
- Nuevo endpoint o accion interna: generar lead report desde `report_request`.
- Render HTML responsive.
- Export opcional a PDF despues, no imprescindible para la primera validacion.

No incluir:

- Analisis largo.
- Todas las citas.
- Demasiados graficos.
- Secciones tecnicas.

Criterios de aceptacion:

- Se genera en menos de 2 minutos para un negocio ya scrapeado.
- Es entendible en menos de 90 segundos.
- Tiene CTA claro a beta mensual.
- No muestra secciones vacias.
- Muestra limitaciones si faltan datos.

### 5. Paid Report Mensual

Modificar el paid report para que parezca un producto recurrente, no un PDF puntual.

Contenido V1:

- Resumen ejecutivo.
- Evolucion mensual si hay historico.
- Comparativa con 5 competidores.
- Riesgos prioritarios.
- Temas positivos y negativos.
- Acciones del mes.
- Plantillas de respuesta.
- Checklist operativo.
- Score por areas:
  - reputacion.
  - visibilidad.
  - conversion.
  - respuesta.
  - oportunidad.

Cambios:

- Nuevo template `paid_report`.
- Nueva entidad `paid_reports`.
- Nuevo campo `report_type`: `lead` o `paid`.
- Hook para generar paid report desde lead o negocio.
- UI CRM: boton "Generar paid report".

Criterios de aceptacion:

- Diferencia visible frente al lead report.
- Incluye competidores.
- Incluye acciones concretas.
- Incluye plantillas reutilizables.
- Puede generarse aunque no exista historico, marcando "primer mes".

### 6. Landing y CTA

Crear una landing operativa para solicitar informe.

Ruta propuesta:

- `/informe-restaurantes-cordoba`

Formulario:

- Nombre del negocio.
- Ciudad.
- URL de Google Maps opcional.
- Email.
- Nombre de contacto opcional.
- Checkbox obligatorio: solicitud de informe.
- Checkbox opcional: comunicaciones comerciales futuras.
- Campos ocultos: UTM, referrer, CTA, landing path.

Copy del CTA:

- "Recibe gratis el diagnostico de tus reseñas".
- "Compara tu negocio con restaurantes similares de Cordoba".
- "Te enviamos 3 oportunidades concretas de mejora".

Cambios:

- Nueva vista publica o ruta en `apps/manager` si se reutiliza el frontend actual.
- Endpoint `POST /report-requests` o `POST /crm/report-requests`.
- Guardado de `consent_proof`.
- Creacion de job local asincrono.

Criterios de aceptacion:

- El formulario crea una solicitud.
- Guarda origen y consentimiento.
- Encola el informe.
- No permite envio sin checkbox obligatorio.
- Marketing futuro queda separado del envio del informe solicitado.

### 7. Cola Local De Informes

Crear flujo local reanudable para generar informes cuando el PC este encendido.

Estados:

- `queued`
- `scraping`
- `analyzing`
- `rendering_lead_report`
- `delivering`
- `delivered`
- `failed`
- `needs_review`

Cambios:

- Nueva entidad `report_requests`.
- Nuevo worker o extension del worker CRM.
- Arranque automatico con el stack actual.
- Reintentos controlados.
- Pantalla CRM: "Solicitudes de informe".

Criterios de aceptacion:

- Si el PC se apaga, el job queda pendiente.
- Al arrancar, se reanudan los `queued` y `failed` recuperables.
- Cada cambio de estado genera evento.
- El usuario puede ver en CRM por que un informe falla.

### 8. Tracking y Atribucion

Cada lead debe guardar de donde viene.

Campos:

- `utm_source`
- `utm_medium`
- `utm_campaign`
- `utm_content`
- `utm_term`
- `cta_id`
- `landing_path`
- `referrer`
- `first_touch_at`
- `last_touch_at`
- `form_id`
- `consent_text_version`

Cambios:

- Captura frontend de parametros UTM.
- Persistencia en `crm_leads` y `report_requests`.
- Filtros CRM por origen.
- Vista agregada simple por canal.

Criterios de aceptacion:

- Una solicitud desde LinkedIn queda marcada como LinkedIn.
- Una solicitud SEO queda marcada como SEO.
- Se puede filtrar por campana.
- El informe generado conserva el origen del lead.

## Tickets Tecnicos

### T00 - Sistema Local De Tareas Y Tablero

Tipo: ops/product.

Tareas:

- Crear fuente de verdad local para tickets del plan.
- Crear CLI para listar, arrancar, bloquear, revisar y cerrar tareas.
- Crear tablero HTML local generado desde las tareas.
- Documentar el flujo obligatorio de implementacion.
- Dejar preparado el flujo para futura sincronizacion con Kanboard/Trello si hace falta.

Aceptacion:

- Existe `docs/tasks/repiq_14d_tasks.json`.
- Existe `scripts/tasks.py`.
- Existe `docs/process/task_workflow.md`.
- Se puede ejecutar `python3 scripts/tasks.py list`.
- Al mover una tarea se actualiza el tablero local en `artifacts/tasks/repiq_14d_board.html`.
- Este ticket queda cerrado usando la propia CLI.

### T01 - Crear Modelos De Benchmark

Tipo: backend.

Tareas:

- Crear contratos para `BenchmarkRun`, `BenchmarkBusiness`, `CompetitorSet`.
- Crear repositorios Mongo.
- Crear indices por `benchmark_id`, `city`, `category`, `rating`, `review_count`.
- Crear tests de repositorio.

Aceptacion:

- CRUD basico cubierto por tests.
- No rompe `crm_leads`.

### T02 - Crear Orquestador De Benchmark

Tipo: backend/worker.

Tareas:

- Crear job `benchmark_local_study`.
- Reutilizar discovery Google Maps live.
- Persistir negocios enriquecidos.
- Asociar resultados a `benchmark_run_id`.

Aceptacion:

- Lanza benchmark desde API o script.
- Genera al menos N negocios para una query controlada.
- Reporta conteos reales.

### T03 - Selector De Competidores

Tipo: backend.

Tareas:

- Implementar scoring de similitud.
- Implementar seleccion determinista.
- Guardar `competitor_sets`.
- Tests con fixtures.

Aceptacion:

- Devuelve competidores razonables.
- No falla con pocos candidatos.

### T04 - Hook De Deep Study

Tipo: analysis.

Tareas:

- Definir schema JSON.
- Implementar builder de snapshot.
- Integrar con prompts existentes si aplica.
- Validar output.

Aceptacion:

- Output estable.
- Warnings cuando faltan datos.

### T05 - Lead Report V1

Tipo: reporting.

Tareas:

- Crear template lead report.
- Renderizar score, media local, oportunidades y CTA.
- Integrar desde `report_request`.
- Tests de template sin contenedores vacios.

Aceptacion:

- HTML generado para fixture completo.
- HTML generado para fixture incompleto.

### T06 - Paid Report V1

Tipo: reporting.

Tareas:

- Crear template paid report.
- Anadir comparativa competidores.
- Anadir acciones mensuales.
- Anadir plantillas de respuesta.

Aceptacion:

- Diferencia clara vs lead report.
- Render robusto con y sin historico.

### T07 - Landing De Solicitud

Tipo: frontend/backend.

Tareas:

- Crear pagina publica.
- Crear endpoint `report_requests`.
- Capturar UTM.
- Guardar consentimiento.
- Encolar job.

Aceptacion:

- Formulario funcional end to end.
- Consentimiento obligatorio para envio del informe.
- Marketing opcional separado.

### T08 - Cola Local Reanudable

Tipo: worker/ops.

Tareas:

- Crear worker para `report_requests`.
- Reanudar jobs pendientes al iniciar.
- Registrar eventos.
- Crear comando/script de smoke test.

Aceptacion:

- Job pendiente se procesa al levantar stack.
- Error queda visible y reintentable.

### T09 - Pantalla CRM: Solicitudes

Tipo: frontend.

Tareas:

- Tabla de solicitudes.
- Filtros por estado, origen y fecha.
- Acciones: ver lead, ver informe, reintentar, marcar needs_review.
- Indicadores de origen.

Aceptacion:

- Se ve el embudo operativo.
- Se puede depurar un informe fallido.

### T10 - Estudio Publico HTML

Tipo: reporting/content.

Tareas:

- Generar pagina HTML del benchmark.
- Mostrar metricas agregadas.
- Mostrar insights anonimizados.
- CTA a landing.

Aceptacion:

- Estudio publicable.
- No expone emails ni datos sensibles.
- CTA trackeable.

## Tickets Editoriales y Distribucion

### E01 - Publicar Estudio En LinkedIn

Canal: LinkedIn personal.

Tareas:

- Post 1: dato fuerte del estudio.
- Post 2: comparativa de patrones positivos/negativos.
- Post 3: caso anonimo de oportunidad concreta.
- Cada post con enlace UTM distinto.

Aceptacion:

- 3 posts publicados.
- Cada enlace tiene `utm_source=linkedin`.
- Se mide visitas y formularios.

### E02 - Crear Pagina SEO Del Estudio

Canal: SEO.

Tareas:

- Publicar pagina: "Estado de las resenas de restaurantes en Cordoba".
- Incluir graficos del benchmark.
- CTA a informe individual.
- Metadata SEO basica.

Aceptacion:

- URL indexable.
- CTA visible.
- `utm_source=seo` para CTAs internos si aplica.

### E03 - Crear 5 Articulos De Apoyo

Canal: SEO tradicional.

Temas:

- Como responder resenas negativas en restaurantes.
- Que significa tener 4,4 estrellas y muchas resenas.
- Errores frecuentes en Google Maps para restaurantes.
- Como mejorar la ficha de Google Business Profile.
- Como comparar tu restaurante con competidores locales.

Aceptacion:

- 5 briefs listos o publicados.
- Cada articulo enlaza al informe.

### E04 - Publicar En Comunidades Locales

Canal: comunidades.

Tareas:

- Identificar grupos de hosteleria Cordoba.
- Publicar el estudio sin spamear.
- Responder comentarios manualmente.
- Usar enlace con `utm_source=community`.

Aceptacion:

- 3 publicaciones o respuestas de valor.
- Registro de canal y enlace usado.

### E05 - Outreach A Partners

Canal: partners.

Targets:

- Agencias web locales.
- Consultores SEO local.
- Fotografos gastronomicos.
- Gestorias de hosteleria.
- Asociaciones de comerciantes.

Tareas:

- Crear mensaje corto de colaboracion.
- Crear link UTM por partner.
- Ofrecer informe gratuito para sus clientes.

Aceptacion:

- 10 partners contactados manualmente.
- Cada partner tiene `cta_id`.

### E06 - Crear Material Para X/Twitter

Canal: X.

Tareas:

- Hilo con 5 aprendizajes del estudio.
- 3 posts cortos con datos.
- Link trackeado.

Aceptacion:

- 1 hilo publicado.
- 3 posts publicados.
- Medicion por UTM.

### E07 - Crear QR Offline

Canal: offline.

Tareas:

- Crear QR hacia landing.
- `utm_source=offline`.
- Usarlo en eventos, asociaciones o visitas no comerciales.

Aceptacion:

- QR generado.
- Landing registra origen offline.

## Calendario De 14 Dias

### Dia 1

- Cerrar T00 y trabajar desde el sistema local de tareas.
- Cerrar nicho.
- Crear modelos de benchmark.
- Crear tracking UTM basico.

### Dia 2

- Orquestador de benchmark.
- Script para lanzar estudio local.
- Primer smoke test con 20 negocios.

### Dia 3

- Discovery completo hasta 100 negocios.
- Persistencia de snapshots.
- Pantalla o endpoint para revisar dataset.

### Dia 4

- Selector de competidores.
- Tests de seleccion.
- Primeras comparativas por negocio.

### Dia 5

- Hook de deep study.
- Scores V1.
- Output JSON validable.

### Dia 6

- Lead report V1.
- Render con fixture real.
- Ajuste de copy.

### Dia 7

- Paid report V1.
- Comparativa y acciones mensuales.
- Diferenciar valor frente al gratuito.

### Dia 8

- Estudio publico HTML.
- Graficos agregados.
- CTA a landing.

### Dia 9

- Landing y formulario.
- Consentimiento separado.
- Endpoint `report_requests`.

### Dia 10

- Worker local de cola.
- Entrega de lead report solicitado.
- Pantalla CRM de solicitudes.

### Dia 11

- Generar 20 lead reports reales.
- Revision manual.
- Ajustar scoring/copy.

### Dia 12

- Publicar estudio SEO.
- Preparar posts LinkedIn/X.
- Crear enlaces UTM.

### Dia 13

- Distribucion organica.
- Comunidades.
- Partners.
- Medicion inicial.

### Dia 14

- Seguimiento de solicitudes.
- Oferta beta 79 EUR/mes.
- Registrar pagos, rechazos y objeciones.

## Metricas De Decision

Continuar si:

- 10+ solicitudes de informe.
- 3+ conversaciones cualificadas.
- 1+ pago o precompromiso.
- Al menos un canal muestra conversion.

Iterar oferta si:

- Hay visitas pero pocos formularios.
- Hay formularios pero nadie considera pagar.
- Los informes gustan, pero el pago mensual no se entiende.

Pausar o pivotar si:

- No hay solicitudes tras distribuir bien.
- Nadie entiende el problema.
- El canal organico no genera ninguna senal despues de probar 3 mensajes.

## Mensaje Comercial A Testar

Version corta:

"Compara tu restaurante con competidores locales y recibe 3 acciones concretas para mejorar tu reputacion en Google este mes."

Version beta:

"Estamos analizando restaurantes de Cordoba. Te enviamos gratis un diagnostico de tus resenas y, si te encaja, puedes activar el radar mensual con comparativa de competidores por 79 EUR/mes."

## Entregables Finales

- `benchmark_run` de 100 restaurantes.
- Estudio publico HTML.
- Landing con CTA.
- Flujo de `report_request`.
- Lead report gratuito.
- Paid report mensual.
- Pantalla CRM de solicitudes.
- Tracking UTM.
- 3 posts LinkedIn.
- 1 pagina SEO.
- 10 contactos partner.
- Decision de continuar, iterar o pivotar.

## Documentacion Relacionada

- `docs/product/benchmark_deep_study_use_cases.md`: casos de uso, diferencias entre benchmark, competidores, deep study, lead report y paid report.
