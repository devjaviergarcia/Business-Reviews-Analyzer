# Fase 0 - Baseline actual Google Maps (documentar y estabilizar)

## Objetivo

Convertir el sistema actual (ya funcional) en una **base de referencia estable** antes de empezar la remodelacion arquitectonica.

Esta fase NO es "descubrir" ni "hacer el MVP". El MVP ya existe. Esta fase sirve para:

- congelar el estado real del producto y del codigo
- documentar limites tecnicos conocidos
- definir que se conserva y que se refactoriza despues
- evitar romper funcionalidades ya demostradas

## Estado de entrada (lo que ya existe)

Ya existe implementado:

- scraping de Google Maps con Playwright
- extraccion de ficha y reseñas
- preprocesado + calculo de estadisticas
- analisis LLM (Gemini)
- reanalisis desde reseñas almacenadas
- MongoDB para persistencia
- FastAPI con endpoints de analisis y lectura
- jobs asincronos con worker y SSE de progreso
- scripts de smoke test y flujo local/docker

## Resultado esperado de la fase

Al terminar esta fase debe quedar una foto clara y util del sistema actual:

- que hace bien
- donde falla
- que partes estan acopladas
- que contratos ya son estables
- que pruebas/manual checks se usan para validar cambios

## Alcance de producto

### Lo que SI se entrega

- definicion funcional del producto actual (que problema resuelve hoy)
- flujo oficial de usuario actual (API + scripts)
- limitaciones funcionales actuales (una fuente principal, salida aun no PDF final)
- criterios de no regresion para fases siguientes

### Lo que NO se hace en esta fase

- integrar nuevas fuentes
- introducir RabbitMQ de forma definitiva
- rehacer la UI
- reescribir toda la arquitectura

## Alcance tecnico

### Artefactos clave

- documentacion contextual (`docs/context/*`)
- scaffold versionado (`scaffold_version.json`)
- mapa de codigo (`scaffold_context.md`)
- lista de riesgos tecnicos actuales
- lista de pruebas smoke/e2e actuales y su uso

### Decisiones que se toman aqui

- que endpoints se consideran contratos provisionales estables
- que campos actuales se mantendran por compatibilidad
- que debt tecnica se acepta temporalmente
- que debt tecnica se ataca en Fase 1 y Fase 2

## Paso a paso (orden recomendado)

### Paso 1 - Inventario funcional del sistema actual

1. Enumerar capacidades reales ya implementadas.
2. Separar capacidades de producto vs utilidades de desarrollo.
3. Identificar que salida produce hoy el sistema para un negocio:
   - listing
   - reviews
   - stats
   - analysis
   - job status/events
4. Documentar que parte es "reporte" hoy:
   - reporte analitico estructurado en JSON/API
   - aun no informe visual final (PDF/Typst)

### Paso 2 - Inventario tecnico del codigo

1. Revisar `src/` y `scripts/` con el scaffold versionado.
2. Marcar responsabilidades actuales por modulo:
   - `scraper/`
   - `pipeline/`
   - `services/`
   - `routers/`
   - `workers/`
3. Detectar acoplamientos fuertes (especialmente en `BusinessService`).
4. Identificar partes reutilizables para fases siguientes.

### Paso 3 - Baseline de operacion y validacion

1. Definir comandos de validacion que siempre deben poder ejecutarse.
2. Separar checks minimos:
   - arranque API
   - conexion Mongo
   - smoke test scraper
   - smoke test LLM
   - endpoint `/business/analyze`
3. Documentar prerequisitos (login, cookies, headless/incognito, docker/local).

### Paso 4 - Baseline de datos y contratos

1. Listar colecciones Mongo actuales y su funcion.
2. Documentar estructura actual de payloads de endpoints principales.
3. Marcar campos "legacy" o transicionales.
4. Definir que contratos deben preservarse durante Fase 1.

### Paso 5 - Registro de riesgos tecnicos

1. Riesgos del scraper (anti-bot, cambios de DOM, sesiones).
2. Riesgos del LLM (coste, latencia, calidad variable).
3. Riesgos de arquitectura (acoplamiento, crecimiento multi-fuente).
4. Riesgos de operacion (docker/headless, perfiles de navegador, retries).

### Paso 6 - Definicion de salida de la fase

1. Actualizar docs de objetivo y fases.
2. Congelar baseline con scaffold versionado.
3. Acordar que empieza exactamente en Fase 1.

## Entregables

- Documentacion del sistema actual (producto + tecnica)
- Roadmap de fases detallado
- Baseline de pruebas/checks
- Lista priorizada de refactors para Fase 1

## Riesgos de esta fase

- confundir "documentar" con "parar el desarrollo"
- documentacion idealizada que no coincide con el codigo real
- mover demasiadas cosas y perder el valor del baseline

## Criterios de salida

- El equipo puede explicar en 10 minutos que hace el sistema hoy y que no hace.
- Existe una lista clara de cambios arquitectonicos para Fase 1.
- Hay comandos de validacion minima para no romper el baseline.
- La documentacion contextual refleja el estado real del codigo.

## Dependencia con la siguiente fase

La Fase 1 usa esta fase como contrato de no regresion: el refactor arquitectonico no puede destruir las capacidades end-to-end ya existentes.
