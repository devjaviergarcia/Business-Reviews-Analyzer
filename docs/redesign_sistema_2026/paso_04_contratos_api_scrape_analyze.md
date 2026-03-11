# Paso 04 - Contratos API (Scrape vs Analyze)

## Objetivo

Separar explicitamente endpoints de scraping y de analisis.

## Endpoint 1: lanzar scraping

`POST /scrape/jobs`

Responsabilidad:

- Crear workflow y scrape jobs.
- Lanzar scraping segun fuentes solicitadas.

Payload:

```json
{
  "workflow_name": "banos_arabes_cordoba_2026_03_11",
  "business_names": {
    "google_maps_name": "Baños Árabes de Córdoba",
    "tripadvisor_name": "Hammam Al Ándalus Córdoba"
  },
  "sources": ["google_maps", "tripadvisor"],
  "force": true,
  "scraper_params": {
    "google": {
      "strategy": "scroll_copy",
      "interactive_max_rounds": null
    },
    "tripadvisor": {
      "max_pages": 3,
      "pages_percent": null
    }
  }
}
```

Respuesta:

```json
{
  "workflow_id": "65f0...001",
  "status": "running_scrape",
  "scrape_jobs": [
    {"job_id": "65f0...002", "source": "google_maps", "status": "queued"},
    {"job_id": "65f0...003", "source": "tripadvisor", "status": "queued"}
  ]
}
```

Comportamiento por fuente:

- Si `sources = ["google_maps"]`, solo crea job Google.
- Si `sources = ["tripadvisor"]`, solo crea job TA.
- Si ambos, crea dos jobs.

## Endpoint 2: lanzar analisis

`POST /analysis/jobs`

Responsabilidad:

- Crear `analysis_job` sobre comentarios ya scrapeados.

Payload:

```json
{
  "workflow_id": "65f0...001",
  "scrape_job_ids": ["65f0...002", "65f0...003"],
  "analysis_config": {
    "batchers": ["latest_text", "balanced_rating"],
    "batch_size": 30,
    "max_reviews_pool": 250
  }
}
```

Reglas:

- Requiere al menos 1 `scrape_job` en `completed`.
- Puede analizar Google solo, TA solo, o ambos.
- Si mezcla fuentes, se analiza union de comentarios.

## Endpoint 3: listar comentarios por job y source

`GET /scrape/jobs/{job_id}/comments?source=tripadvisor&page=1&page_size=50`

Respuesta:

```json
{
  "job_id": "65f0...003",
  "source": "tripadvisor",
  "items": [
    {
      "comment_id": "65f0...101",
      "job_id": "65f0...003",
      "source": "tripadvisor",
      "text": "Muy buena experiencia...",
      "rating": 5.0
    }
  ],
  "page": 1,
  "page_size": 50,
  "total": 120
}
```

## Endpoint 4: estado de workflow y nodos

`GET /workflows/{workflow_id}`

Incluye:

- estado workflow;
- scrape jobs;
- analysis jobs;
- resumen de contadores.

## Endpoint 5: eventos en tiempo real

`GET /workflows/{workflow_id}/events` (SSE)

Eventos:

- `workflow_status_changed`
- `scrape_job_status_changed`
- `analysis_job_status_changed`
- `tripadvisor_session_state_changed`
- `needs_human_required`

## Endpoint 6: acciones manuales

- `POST /scrape/jobs/{job_id}/relaunch`
- `POST /scrape/jobs/{job_id}/mark-needs-human`
- `POST /scrape/jobs/{job_id}/confirm-human-done`

## Compatibilidad

Transicion recomendada:

- Mantener temporalmente `/business/analyze` y `/business/analyze/queue`.
- Internamente redirigir a nuevo flujo:
  - primero `scrape/jobs`;
  - despues `analysis/jobs`.

