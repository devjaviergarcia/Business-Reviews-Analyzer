# V211 - Infra Hibrida Segura con Supabase Queue (Vercel + Worker local en PC)

## Objetivo

Permitir que la landing en Vercel reciba solicitudes 24/7 aunque tu PC este apagado,
con este flujo unico:

`landing -> Supabase (queue/persistencia) -> worker local por pull -> CRM local -> artefactos`

Sin puertos entrantes en casa, con trazabilidad completa y control legal.

## Decision de arquitectura (cerrada para V211)

- Front/control-plane: **Vercel + Supabase**
- Ejecucion pesada (scraping/LLM/report): **PC local**
- Patron de comunicacion: **pull desde PC** (nunca push directo a tu PC)
- Persistencia intermedia obligatoria: **Supabase** (no depender de localhost)

## Problema que resuelve

Actualmente la landing intenta hablar con API/CRM local. Eso no escala porque Vercel
no puede alcanzar `localhost` de tu casa.

Con Supabase:
- la solicitud siempre se guarda,
- tu PC la procesa cuando este online,
- no se pierden leads por apagar el PC.

## Alcance

Incluye:
- Ingreso de leads/jobs desde landing y desde alta manual.
- Cola remota en Supabase con estados y reintentos.
- Worker local daemon que hace poll/claim/execute/ack.
- Sincronizacion de progreso y resultado.
- Registro legal (consentimientos/version/timestamp/origen).
- Indice de artefactos y entrega por URL firmada.

No incluye (se deja para fase posterior):
- WhatsApp Business API totalmente automatizada.
- Reescritura completa del CRM actual.

---

## Flujo E2E oficial

## Flujo A: Lead desde formulario web

1. Usuario envia formulario en `repiq.es`.
2. Vercel valida (`zod`, rate limit, Turnstile).
3. Vercel inserta en Supabase:
   - `intake_requests` (payload completo + legal + metadata)
   - `job_queue` (job tipo `lead_intake` en estado `pending`)
4. Vercel envia email operativo a Javier (con `request_id` y payload JSON).
5. Worker local (PC) hace poll cada X segundos.
6. Worker ejecuta `claim` atomico de un job `pending`.
7. Worker procesa:
   - crea/actualiza lead en CRM local,
   - dispara pipeline si aplica,
   - genera artefactos.
8. Worker reporta progreso/eventos a Supabase.
9. Worker marca job `completed` (o `retry_wait` / `failed` / `dead_letter`).
10. CRM/panel consulta estados desde Supabase para visibilidad global.

## Flujo B: Job manual interno

1. Desde CRM/panel se crea un job manual.
2. Se guarda en la **misma** `job_queue` de Supabase.
3. Worker local lo consume igual que cualquier job de landing.

Regla: **un solo pipeline y una sola cola** para todo.

---

## Modelo de datos minimo (Supabase)

## Tabla `intake_requests`

Campos recomendados:
- `id` uuid pk
- `created_at` timestamptz
- `source` text (`landing`, `linkedin`, `cold_whatsapp`, etc.)
- `business_name` text
- `contact_name` text nullable
- `delivery_channel` text (`whatsapp|email`)
- `phone` text nullable
- `email` text nullable
- `city` text nullable
- `message` text nullable
- `utm` jsonb
- `page_context` jsonb (source_page, referrer)
- `legal` jsonb (consents, legal_version, consent_timestamp, ip_hash)
- `status` text (`pending`, `processed`, `error`)
- `error_last` text nullable

## Tabla `job_queue`

Campos recomendados:
- `id` uuid pk
- `created_at` timestamptz
- `updated_at` timestamptz
- `type` text (`lead_intake`, `report_generation`, ...)
- `source` text (`landing_form`, `manual_crm`)
- `payload` jsonb
- `status` text
  - `pending`
  - `claimed`
  - `running`
  - `uploading`
  - `completed`
  - `retry_wait`
  - `failed`
  - `dead_letter`
  - `cancelled`
- `priority` int default 100
- `attempt_count` int default 0
- `max_attempts` int default 5
- `next_retry_at` timestamptz nullable
- `claim_token` uuid nullable
- `claimed_by` text nullable
- `claimed_at` timestamptz nullable
- `lease_until` timestamptz nullable
- `last_heartbeat_at` timestamptz nullable
- `idempotency_key` text unique
- `result_ref` jsonb nullable
- `error_last` text nullable

## Tabla `job_events`

- `id` bigserial pk
- `job_id` uuid fk
- `created_at` timestamptz
- `event_type` text (`claimed`, `progress`, `upload_done`, `completed`, ...)
- `progress_pct` numeric nullable
- `message` text nullable
- `meta` jsonb

## Tabla `artifacts_index`

- `id` uuid pk
- `job_id` uuid fk
- `kind` text (`pdf`, `html`, `json`)
- `path` text
- `sha256` text
- `bytes` bigint
- `created_at` timestamptz
- `signed_url_expires_at` timestamptz nullable

---

## RPC/operaciones criticas en Supabase

Para evitar carreras, definir RPC SQL:

- `claim_next_job(worker_id text, lease_seconds int)`
  - Hace lock atomico y devuelve 1 job o null.
- `heartbeat_job(job_id uuid, claim_token uuid, lease_seconds int)`
  - Extiende lease solo si token coincide.
- `complete_job(job_id uuid, claim_token uuid, result_ref jsonb)`
- `fail_job(job_id uuid, claim_token uuid, error text, retry_delay_seconds int)`
  - Si supera `max_attempts` -> `dead_letter`.
- `requeue_stale_jobs()`
  - Mueve `claimed/running` sin heartbeat a `pending`.

---

## Worker local (PC) - contrato operativo

Daemon (systemd) que hace:

1. `poll` cada 5-15s.
2. `claim_next_job`.
3. Si hay job:
   - `status=running`
   - ejecuta pipeline local
   - heartbeat cada 10-20s
4. Si genera artefacto:
   - sube a storage (S3 compatible)
   - guarda hash/index
5. `complete_job` o `fail_job`.

Requisitos:
- Idempotencia por `idempotency_key`.
- Resume tras reinicio del PC.
- Logs estructurados por `job_id`.

---

## Seguridad

## Secretos

- En Vercel:
  - `SUPABASE_URL`
  - `SUPABASE_ANON_KEY` (solo cliente si aplica)
  - `SUPABASE_SERVICE_ROLE_KEY` (solo server routes)
  - `TURNSTILE_SECRET_KEY`
  - `RESEND_API_KEY`
- En PC local:
  - `SUPABASE_URL`
  - `SUPABASE_SERVICE_ROLE_KEY` (worker)
  - `STORAGE_ACCESS_KEY`, `STORAGE_SECRET_KEY`

Reglas:
- Nunca commitear `.env`.
- Rotacion trimestral o inmediata ante incidente.
- Principle of least privilege por componente.

## Red

- Tu PC solo hace salida HTTPS a Supabase/storage.
- No abrir puertos en router domestico.

---

## Consentimiento legal (bloqueante)

Cada solicitud debe guardar:
- `consent_contact` (true/false)
- `consent_privacy`
- `consent_terms`
- `consent_marketing`
- `legal_version`
- `consent_timestamp`
- `consent_origin`
- `consent_ip_hash`

Politica:
- Sin consentimiento valido no se habilita contacto outbound.

---

## Observabilidad y SLO minimo

Metricas:
- `queue_pending_count`
- `queue_retry_wait_count`
- `queue_dead_letter_count`
- `job_latency_p50/p95`
- `% success vs failed`

Alertas:
- pending > umbral durante > X min
- dead_letter > 0
- worker sin heartbeat global > X min

---

## Plan de implementacion (subtareas V211.x)

- **V211.1** Schema SQL en Supabase (`intake_requests`, `job_queue`, `job_events`, `artifacts_index`).
- **V211.2** RPC atomicas de cola (`claim/heartbeat/complete/fail/requeue_stale`).
- **V211.3** Adaptar `/api/form-pre` (Vercel) para persistir en Supabase + encolar + email.
- **V211.4** Worker local daemon (`poll -> claim -> execute -> ack`) con systemd.
- **V211.5** Integrar pipeline local actual (scrape/llm/report) sobre jobs de cola.
- **V211.6** Storage de artefactos + hash + index.
- **V211.7** Sincronizacion de estado/progreso para CRM/panel.
- **V211.8** Hardening de secretos, idempotencia y runbooks de incidentes.
- **V211.9** Prueba E2E completa (landing y job manual en pipeline unico).

---

## Definition of Done (DoD)

Se considera cerrado cuando:

1. Formulario de landing funciona con PC apagado y guarda solicitud en Supabase.
2. Al encender PC, worker consume pendientes automaticamente.
3. El job cambia de estado con trazabilidad completa (`job_events`).
4. Errores recuperables van a `retry_wait`; agotados a `dead_letter`.
5. Artefactos quedan indexados con hash y accesibles por URL firmada.
6. Consentimiento legal queda persistido y auditable por `request_id`.
7. Existe runbook probado para:
   - rotacion de secretos,
   - recovery de jobs atascados,
   - replay de dead letter.

---

## Runbooks minimos

- Bootstrap de worker local nuevo.
- Rotacion de `SUPABASE_SERVICE_ROLE_KEY`.
- Procedimiento de `dead_letter replay`.
- Incidencia: PC offline 24h.
- Incidencia: caida temporal de storage.
