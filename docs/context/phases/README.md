# Directorio de Fases

Este directorio describe el roadmap del proyecto en fases ejecutables, con enfoque de producto y tecnico.

## Como leer estas fases

Cada fase incluye:

- objetivo de negocio
- estado de entrada (que debe existir antes de empezar)
- alcance de producto
- alcance tecnico
- paso a paso de implementacion (orden recomendado)
- entregables
- riesgos
- criterios de salida
- dependencia con fases posteriores

## Fases del roadmap actual

0. `fase_00_baseline_actual_google_maps.md`
1. `fase_01_arquitectura_modular_y_workers.md`
2. `fase_02_modelo_canonico_negocio_y_fuentes.md`
3. `fase_03_scraper_tripadvisor.md`
4. `fase_04_scraper_trustpilot.md`
5. `fase_05_scraper_reddit.md`
6. `fase_06_refinamiento_analisis_llm_rag.md`
7. `fase_07_informe_estructurado.md`
8. `fase_08_interfaz_mvp_analisis.md`
9. `fase_09_landing_demo_y_email.md`

## Regla practica de planificacion

No empezar una fase por complejidad tecnica pura. Empezar cuando:

- existe contrato de entrada/salida claro
- se conoce como se valida (test/smoke/e2e)
- existe una definicion de "hecho" util para negocio

## Relacion con la documentacion contextual

- `docs/context/project_objective.md`: vision global y estado actual
- `docs/context/architecture/README.md`: arquitectura objetivo y scaffolding
- `docs/context/architecture/scaffold_context.md`: mapa del codigo real en cada version
