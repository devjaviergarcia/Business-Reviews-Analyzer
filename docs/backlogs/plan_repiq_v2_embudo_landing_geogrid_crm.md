# Plan Repiq V2 - Landing, Embudo, GeoGrid y CRM

## Objetivo

Aterrizar el embudo comercial completo de Repiq sobre una nueva capa web
(Next.js) y conectar ese embudo con el pipeline actual de informes/geogrid
para pasar de prospeccion manual a cierre trazable por etapas.

## Replan vigente (manual-first)

Para validar mercado sin sobredesarrollar automatizaciones, el orden vigente
prioriza flujo manual:

- Captacion por formulario con cumplimiento legal y antispam.
- Notificacion server-side a Javier con datos estructurados.
- Alta y gestion manual de lead en CRM.
- Automatizaciones completas (post-informe/envio auto) en fase posterior.

Referencia de ejecucion: `docs/backlogs/epic_manual_funnel_v1.md`.

## Etapas CRM (fuente de verdad)

`PROSPECTO -> CONTACTADO -> LEAD_REPORT_SENT -> FORM_1_DONE -> FULL_REPORT_SENT -> FORM_2_DONE -> CLIENTE`

Estado alterno global:
`LOST` (con motivo y subestado `RECUPERABLE` cuando aplique).

## Tickets

### V201 - Setup del proyecto Next.js

- Crear base `repiq-web` con App Router, Tailwind, RHF, Zod, Framer Motion.
- Preparar estructura de carpetas y layout global mobile-first.
- Configurar metadatos OG y variables CSS de branding.

Aceptacion:
- Proyecto arranca en local y build pasa.
- Estructura de carpetas creada segun blueprint.
- Layout y CSS globales listos para montar landing y formularios.

### V202 - Landing principal de conversion

- Implementar pagina unica `app/page.tsx` con Hero, HowItWorks,
  WhatIncludes y FinalCTA.
- CTA principal con scroll al formulario.
- Copys alineados con oferta de informe gratuito.

Aceptacion:
- Landing completa operativa en movil.
- CTA principal funcional.
- Secciones y textos definidos en este plan visibles.

### V203 - Formulario de cualificacion (Form 1)

- Implementar wizard de 5 pasos con RHF + Zod.
- API `POST /api/form-pre` valida, crea/actualiza lead y mueve etapa a `FORM_1_DONE`.
- Guardar canal/contacto y disparar envio del informe grande.

Aceptacion:
- Flujo Form 1 completo funcionando extremo a extremo.
- Etapa CRM se actualiza automaticamente.
- Notificacion a Javier enviada al completar.

### V204 - Formulario post-informe (Form 2)

- Implementar ruta `/valoracion?lid=...` con logica condicional ramas A/B/C.
- API `POST /api/form-post` actualiza etapa `FORM_2_DONE` y etiqueta
  `HOT_LEAD | WARM_LEAD | COLD | RECUPERABLE`.
- Notificar a Javier con resumen de respuestas.

Aceptacion:
- Ramas A/B/C funcionales.
- Etiquetado y etapa persistidos en BD.
- Notificacion emitida con etiqueta visible.

### V205 - GeoGrid en informe grande

- Crear modulo `geogrid` (grid, scanner, renderer).
- Añadir seccion "Visibilidad geografica" al informe final.
- Generar insight sintetico sobre cobertura local.

Aceptacion:
- Informe grande muestra heatmap + score + insight.
- Resultado geogrid cacheable para evitar recomputo inmediato.
- Se mantiene compatibilidad con pipeline actual.

### V206 - Feature flag onboarding_form_url

- Integrar bandera `onboarding_form_url` en informe grande.
- Si existe URL, renderizar CTA al Form 2; si no, ocultar bloque.
- Construir URL con `lead_id` parametrizado.

Aceptacion:
- CTA aparece/desaparece por feature flag.
- URL incluye `lid` y abre formulario correcto.

### V207 - Envio automatico informe grande

- Orquestar envio desde Form 1: `FORM_1_DONE -> FULL_REPORT_SENT`.
- Integrar `report-sender.ts` con API Python de generacion.
- Entrega segun canal elegido (whatsapp/email), aunque sea semiautomatica.

Aceptacion:
- Trigger automatico tras Form 1.
- Etapa `FULL_REPORT_SENT` persistida.
- Error handling y notificacion de fallo a Javier.

### V208 - Componente OptionCard reusable

- Crear `OptionCard` para seleccion unica y multiple.
- Unificar estados visuales (normal, hover, seleccionado).
- Reutilizar en Form 1 y Form 2.

Aceptacion:
- Componente reutilizado en ambos formularios.
- Estados visuales y transiciones aplicadas.

### V209 - CRM de estados en /admin (kanban)

- Implementar panel interno con columnas por etapa del embudo.
- Soportar cambio manual de etapa/label, notas, motivo LOST.
- Exponer APIs de crear lead, actualizar estado y listar por etapa.

Aceptacion:
- Kanban funcional con datos reales.
- Trazabilidad por timestamps de etapa.
- Operativa diaria de Javier cubierta sin tooling externo.

### V210 - Sistema de notificaciones (notify.ts)

- Centralizar notificaciones de cambios de etapa y formularios.
- Envio por Resend hacia Javier.
- Tipado comun de `LeadStage` y `LeadLabel`.

Aceptacion:
- Notificaciones enviadas desde Form 1, Form 2 y cambios clave.
- Manejador desacoplado de API routes.

### V211 - Infra hibrida segura (VPS control-plane + PC worker local)

- Definir y desplegar arquitectura hibrida donde scraping, LLM y generacion de PDFs
  viven en el PC local, sin abrir puertos entrantes en casa.
- Separar claramente:
  - Control-plane (VPS): landing, API publica, CRM, autenticacion, cola de jobs y estado.
  - Data-plane (PC local): workers de ejecucion, Playwright, LLM, almacenamiento local temporal.
- Implementar flujo dual:
  - `landing -> enqueue remoto -> pull local -> procesamiento -> subida artefactos -> callback estado`.
  - `lanzamiento manual (ya existente) -> encola/persiste en mismo pipeline comun`.
- Introducir consentimiento y cumplimiento:
  - Registro de aceptacion de terminos/privacidad para contacto por WhatsApp o email.
  - Trazabilidad de timestamp, origen, version de texto legal y canal autorizado.
- Definir almacenamiento de artefactos:
  - salida temporal local en PC
  - persistencia en object storage barato (S3 compatible) con URLs firmadas.
- Endurecer seguridad extremo a extremo:
  - autenticacion machine-to-machine para worker local (token rotativo o mTLS)
  - cifrado en transito, secretos fuera de repo, firma/HMAC de callbacks
  - permisos minimos por servicio y auditoria de eventos.
- Añadir resiliencia operativa:
  - leases/heartbeats de jobs, reintentos idempotentes, dead-letter queue y replay.
  - recuperacion automatica cuando el PC vuelva a encenderse.
  - observabilidad minima: health checks, metricas y alertas.

Aceptacion:
- Existe documento de arquitectura objetivo con diagrama de componentes, trust boundaries y secuencia de eventos.
- Se despliega control-plane en VPS y acepta jobs desde landing y desde trigger manual.
- Worker local ejecuta en modo pull sin puertos entrantes y procesa cola pendiente al arrancar.
- El estado de job/progreso/resultado se refleja en CRM de forma consistente.
- Los PDFs y artefactos se guardan en storage remoto con acceso mediante URL firmada y expiracion.
- Consentimiento legal queda persistido por lead (canal, texto, version, timestamp e IP hash/ancla de origen).
- Callbacks/API entre VPS y worker local usan autenticacion fuerte y validacion de integridad.
- Existen runbooks: bootstrap del worker local, rotacion de secretos, recovery de jobs atascados y procedimiento de incidentes.
- Se ejecuta prueba E2E de los 2 casos de uso: (1) job desde landing, (2) job manual.

## Orden recomendado

Semana 1:
- V201, V208, V210

Semana 2:
- V209, V203

Semana 3:
- V202, V204

Semana 4:
- V205, V206, V207

Semana 5:
- V211
