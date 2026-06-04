# EPICA - Manual Funnel V1 (Validacion rapida)

## Objetivo

Construir un flujo comercial **manual-first** para validar conversion real sin sobre-automatizar:

- Captar leads desde landing con consentimiento legal.
- Recibir esos datos por email server-side (formato util para operacion).
- Crear/gestionar lead en CRM manualmente (incluyendo leads de prospeccion en frio).
- Mover lead por etapas del funnel de forma simple y trazable.

## Principio operativo

Primero validamos demanda y cierre manual.
Despues automatizamos envio de informes y orquestacion completa.

## Secuencia de ejecucion (orden estricto)

1. `V201` - Setup base Next.js y branding.
2. `V212` - Definir epica y contratos funcionales manual-first.
3. `V213` - Legal/consentimiento (terminos, privacidad, checkbox, versionado legal).
4. `V214` - Formulario completo de captacion (telefono/email + origen).
5. `V216` - Antispam y hardening de entrada (rate limiting + Turnstile).
6. `V215` - Envio server-side al correo de Javier (texto + payload estructurado).
7. `V221` - Persistencia automatica en BD desde `form-pre` (ademas del email).
8. `V217` - Importador rapido de lead en CRM desde payload copiado del email.
9. `V218` - Gestion de etapas del funnel + alta manual de leads en frio.
10. `V219` - Despliegue y checklist operacional (claves, cuentas, DNS, runbook).
11. `V220` - Editorial/CTA alineado al flujo manual.
12. `V222` - Formulario final de reporte (`/valoracion`) con guardado real.
13. `V203/V207/V204` - Automatizaciones posteriores (fase 2), tras validar mercado.
14. `V223` - Optimizacion SEO basada en analisis del audio de Reo + guia oficial de Google para AI Search.

## Casos de uso cubiertos

## Caso A - Lead entra por formulario web

Landing -> formulario -> validacion legal/antibot -> persistencia en BD + email a Javier -> (opcional) import manual via payload -> seleccionar etapa inicial del funnel -> seguimiento manual.

## Caso B - Lead entra por prospeccion en frio

Javier detecta negocio (scraping/telefono/WhatsApp) -> alta manual en CRM -> seleccion de etapa y notas -> seguimiento manual.

## Datos minimos que debe guardar un lead

- Nombre negocio
- Nombre contacto (si existe)
- Telefono
- Email
- Canal preferido de envio (`whatsapp|email`)
- Ciudad/zona
- Origen (`landing|linkedin|seo|cold_call|cold_whatsapp|otro`)
- Estado funnel actual
- Consentimiento (`aceptado`, `version`, `timestamp`, `origen`)
- Notas operativas

## Requisitos legales minimos en landing

- Politica de privacidad accesible.
- Terminos de servicio accesibles.
- Checkbox explicito no pre-marcado para tratamiento de datos.
- Texto legal para contacto por WhatsApp/email.
- Registro de version legal aceptada.

## Lo que debes aportar (owner checklist)

## Cuentas y claves

- Vercel (proyecto + variables entorno)
- Dominio `repiq.es` (DNS en proveedor)
- Servicio email transaccional (Resend recomendado)  
  Alternativa: SMTP Gmail Workspace si prefieres.
- Cloudflare Turnstile (site key + secret)

## Configuracion y legales

- Email destino de notificaciones operativas.
- Textos finales de Terminos y Privacidad (o borrador legal revisado).
- Lista de canales de origen que quieres trackear por defecto.
- Definicion final de etapas del funnel y nombres visibles en CRM.

## Definicion del funnel manual V1

Etapas recomendadas V1:

`PROSPECTO -> CONTACTADO -> LEAD_REPORT_SENT -> INTERESADO -> NEGOCIACION -> CLIENTE`

Estado alterno:

`LOST` (con motivo)

Nota: si quieres mantener exactamente el funnel largo ya definido, se puede mapear sin problema.

## Criterio de salida de la epica

- Entran leads por formulario con proteccion antiabuso y consentimiento legal trazable.
- Cada formulario queda persistido en BD aunque falle el correo.
- Javier recibe correo server-side estructurado util para operacion.
- Javier puede crear lead rapido en CRM desde payload pegado.
- CRM permite crear lead manual de prospeccion en frio y moverlo entre etapas.
- El formulario final de reporte (`/valoracion`) queda guardado y asociado al lead.
- Flujo probado E2E en produccion con al menos 5 leads reales.
- Quedan listos los siguientes tickets de automatizacion (fase 2).
