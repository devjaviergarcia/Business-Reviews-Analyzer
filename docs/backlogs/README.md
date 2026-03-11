# Backlogs por fase

Directorio de backlogs operativos del roadmap. Cada archivo aterriza la fase en epicas y tickets ejecutables.

## Convenciones

- IDs de epicas: `E##-XX`
- IDs de tickets: `T##-XXX`
- Estado inicial sugerido: `TODO`
- Mantener IDs cuando migres tickets a GitHub Issues/Jira/Linear
- Leer primero la fase en `docs/context/phases/` y despues este backlog

## Indice

| Fase | Archivo | Epicas | Tickets | Documento de fase |
| --- | --- | ---: | ---: | --- |
| `fase_00` | `fase_00_baseline_actual_google_maps_backlog.md` | 4 | 12 | `docs/context/phases/fase_00_baseline_actual_google_maps.md` |
| `fase_01` | `fase_01_arquitectura_modular_y_workers_backlog.md` | 4 | 12 | `docs/context/phases/fase_01_arquitectura_modular_y_workers.md` |
| `fase_02` | `fase_02_modelo_canonico_negocio_y_fuentes_backlog.md` | 4 | 12 | `docs/context/phases/fase_02_modelo_canonico_negocio_y_fuentes.md` |
| `fase_03` | `fase_03_scraper_tripadvisor_backlog.md` | 4 | 12 | `docs/context/phases/fase_03_scraper_tripadvisor.md` |
| `fase_04` | `fase_04_scraper_trustpilot_backlog.md` | 4 | 12 | `docs/context/phases/fase_04_scraper_trustpilot.md` |
| `fase_05` | `fase_05_scraper_reddit_backlog.md` | 4 | 12 | `docs/context/phases/fase_05_scraper_reddit.md` |
| `fase_06` | `fase_06_refinamiento_analisis_llm_rag_backlog.md` | 4 | 12 | `docs/context/phases/fase_06_refinamiento_analisis_llm_rag.md` |
| `fase_07` | `fase_07_informe_estructurado_backlog.md` | 4 | 12 | `docs/context/phases/fase_07_informe_estructurado.md` |
| `fase_08` | `fase_08_interfaz_mvp_analisis_backlog.md` | 4 | 12 | `docs/context/phases/fase_08_interfaz_mvp_analisis.md` |
| `fase_09` | `fase_09_landing_demo_y_email_backlog.md` | 4 | 12 | `docs/context/phases/fase_09_landing_demo_y_email.md` |

## Uso recomendado

1. Selecciona la fase activa y revisa dependencias.
2. Pasa epicas/tickets al gestor que uses manteniendo IDs.
3. Actualiza estados reales (`TODO`, `IN_PROGRESS`, `BLOCKED`, `DONE`).
4. Al cerrar una fase, actualiza `docs/context/project_objective.md` y este backlog.
