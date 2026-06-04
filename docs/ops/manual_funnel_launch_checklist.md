# Manual Funnel V1 - Checklist de lanzamiento

## 1) Cuentas necesarias

- [ ] Vercel (proyecto `repiq-web` o equivalente)
- [ ] Proveedor DNS del dominio `repiq.es`
- [ ] Servicio de correo transaccional (recomendado: Resend)
- [ ] Cloudflare Turnstile

## 2) Variables de entorno (Vercel / local)

- [ ] `NEXT_PUBLIC_BASE_URL`
- [ ] `RESEND_API_KEY` (o SMTP equivalente)
- [ ] `MAIL_FROM`
- [ ] `MAIL_TO_JAVIER`
- [ ] `TURNSTILE_SITE_KEY`
- [ ] `TURNSTILE_SECRET_KEY`
- [ ] `FORM_RATE_LIMIT_WINDOW_SEC`
- [ ] `FORM_RATE_LIMIT_MAX_REQUESTS`

## 3) Legal y cumplimiento

- [ ] URL publica de Politica de Privacidad
- [ ] URL publica de Terminos
- [ ] Texto de consentimiento para contacto por WhatsApp/email
- [ ] Version legal vigente (ej. `legal_v1_2026_05`)
- [ ] Registro de version legal en backend

## 4) Formulario y captacion

- [ ] Campo telefono o email obligatorio segun canal
- [ ] Campo origen del lead (`landing/linkedin/seo/cold/...`)
- [ ] Confirmacion post-envio con expectativa realista (revision manual)

## 5) Seguridad antiabuso

- [ ] Rate limiting server-side habilitado
- [ ] Turnstile validado server-side
- [ ] Logs de bloqueos/429 visibles

## 6) Correo operativo a Javier

- [ ] Email llega en < 60s tras envio
- [ ] Incluye resumen legible para accion rapida
- [ ] Incluye payload JSON para copiar/pegar a CRM

## 7) CRM manual

- [ ] Alta rapida desde payload pegado
- [ ] Alta manual de lead en frio (telefono/WhatsApp)
- [ ] Cambio de etapa del funnel con timestamp y nota
- [ ] Filtro por etapa y origen

## 8) Prueba de humo E2E

- [ ] Caso A: lead desde landing -> correo recibido -> alta en CRM
- [ ] Caso B: lead en frio -> alta manual -> cambio de etapa
- [ ] Capturas/logs guardados como evidencia

## 9) Go/No-Go

- [ ] Si todo OK: publicar CTA en LinkedIn/SEO
- [ ] Si falla correo o antiabuso: no abrir trafico hasta corregir
