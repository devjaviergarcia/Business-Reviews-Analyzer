# Objetivo del Proyecto

## Que es

Sistema de inteligencia de reputacion para negocios locales que:

1. Recopila datos publicos de reputacion (reviews y menciones) desde multiples fuentes.
2. Normaliza esos datos a un modelo canonico centrado en un negocio.
3. Ejecuta analisis con LLM para convertir ruido en hallazgos accionables.
4. Genera informes estructurados para el empresario.
5. Expone todo por API, jobs asincronos y (en fases posteriores) UI web.

## Que hace hoy (estado real implementado)

Actualmente ya existe una base funcional importante (lo que consideramos la Fase 0 de este roadmap):

- Scraper de Google Maps con Playwright (con selectores dinamicos y mitigaciones anti-bot).
- Extraccion de ficha del negocio y reseñas.
- Preprocesado de reseñas y calculo de estadisticas.
- Analisis con Gemini (LLM) y reanalisis con diferentes batchers.
- Persistencia en MongoDB (`businesses`, `reviews`, `analyses`, `analysis_jobs`).
- API FastAPI para analizar, consultar negocios/reviews/analisis y reanalizar.
- Jobs asincronos con cola persistida y eventos SSE de progreso.
- Scripts de smoke test / flujo local / docker.

Importante: hoy el sistema es funcional, pero esta acoplado principalmente a Google Maps y a una arquitectura de servicio unica (`BusinessService`).

## Vision de producto (objetivo de medio plazo)

Construir una plataforma de analisis reputacional multi-fuente para negocios que permita:

- analizar rapidamente un negocio con una vision de 360 grados (Google, Trustpilot, Tripadvisor, Reddit, etc.)
- comparar percepcion por fuente y por tema
- recibir recomendaciones accionables y respuestas sugeridas
- generar informes profesionales compartibles (PDF/Typst)
- operar el pipeline en segundo plano con workers especializados

## Fase actual del roadmap

**Fase 0 (baseline actual consolidado)**

El foco inmediato no es "demostrar el MVP" (eso ya esta hecho), sino **documentar y congelar el baseline** para poder refactorizar la arquitectura sin perder funcionalidades.

## Arquitectura objetivo (alto nivel)

Pipeline con 3 workers especializados:

1. `scrape-worker`
2. `analysis-worker`
3. `report-worker`

Con orquestacion por mensajes (RabbitMQ + workers propios) y un modelo canonico de negocio + fuente + menciones/reviews.

## Roadmap de fases (nuevo)

0. Baseline actual Google Maps + API + jobs (documentar y estabilizar lo construido)
1. Remodelado de arquitectura modular + workers + RabbitMQ (diseno e implementacion base)
2. Modelo canonico de negocio/fuentes y estructura de datos multi-fuente (incluyendo datos extra de entrada)
3. Integracion scraper de Tripadvisor
4. Integracion scraper de Trustpilot
5. Integracion scraper de Reddit
6. Refinamiento del analisis (prompting, RAG, modos de analisis, outputs)
7. Generacion de informe estructurado (PDF/Typst/plantillas)
8. Interfaz MVP de analisis (formulario, progreso, visualizacion de informe)
9. Landing de demo + captura de leads + email con resumen inicial

## Principio de trabajo para todas las fases

Cada fase se debe ejecutar con la misma disciplina:

- definir objetivo de negocio
- disenar contratos de datos
- implementar incrementalmente
- instrumentar progreso/errores
- validar con smoke tests o e2e
- documentar la fase (producto + tecnica + criterios de salida)
