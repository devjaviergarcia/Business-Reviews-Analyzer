# Fase 8 - Interfaz MVP de analisis (formulario, progreso, informe)

## Objetivo

Construir una interfaz MVP que permita a un usuario no tecnico lanzar un analisis, seguir el progreso y visualizar el informe generado.

Esta fase convierte la API en experiencia de producto usable.

## Alcance de producto

### Flujo objetivo de usuario

1. Introduce datos del negocio (nombre + datos enriquecidos opcionales)
2. Selecciona tipo de analisis (demo/profundo/etc.)
3. Lanza el job
4. Ve progreso en tiempo real
5. Visualiza informe y/o descarga PDF

### Funcionalidades MVP incluidas

- formulario de analisis
- estado/progreso del job
- visualizacion de resultado/informe
- manejo de errores comprensible

### Funcionalidades fuera de alcance (por ahora)

- autenticacion multiusuario completa
- panel historico avanzado
- billing o planes
- configurador complejo de plantillas

## Alcance tecnico

### Backend (ya existente + ajustes)

- endpoints de creacion de job/analisis
- eventos de progreso (SSE)
- endpoint de consulta de resultado/informe
- endpoints para descargar artefacto de reporte

### Frontend MVP (tecnologia a elegir)

Opciones pragmáticas:

- React/Vite (rapido para iterar)
- Next.js si quieres pensar ya en futura landing/app juntas

Recomendacion: empezar con una app simple separada (React/Vite) para acelerar iteracion.

## Paso a paso (orden recomendado)

### Paso 1 - Diseñar flujo UX minimo

Definir pantallas/estados:

- pantalla formulario
- pantalla "job en progreso"
- pantalla de resultado
- estados de error/reintento

Evitar construir dashboard complejo en esta fase.

### Paso 2 - Definir payload de entrada del formulario

El formulario debe soportar datos enriquecidos (Fase 2):

- nombre del negocio
- direccion (opcional)
- ciudad (opcional)
- pais (opcional)
- idioma (opcional)
- website (opcional)
- modo de analisis (demo/profundo/...)
- fuentes habilitadas (si ya aplica)

### Paso 3 - Integrar creacion de job

1. UI llama a endpoint de analisis (sync o async, recomendado async).
2. Guarda `job_id` en estado local.
3. Navega a vista de progreso.

### Paso 4 - Integrar barra de progreso y estados

Usar SSE (o polling como fallback):

- mostrar etapa actual
- mostrar mensajes de progreso por worker/fase
- mostrar errores intermedios si una fuente falla
- indicar finalizacion correcta o fallo

Objetivo: no dejar al usuario mirando un spinner sin contexto.

### Paso 5 - Visualizacion de resultado e informe

Mostrar al menos:

- resumen ejecutivo
- sentimientos/temas principales
- fortalezas/debilidades
- acciones sugeridas
- estado y fuentes usadas
- enlace de descarga del informe (si ya se genera)

### Paso 6 - Manejo de errores y recuperacion

Definir UX de errores:

- validacion de input
- fallo de scraping de una fuente
- timeout del job
- error de analisis LLM
- informe no disponible aun

Mostrar mensajes accionables, no errores crudos del backend.

### Paso 7 - Instrumentacion MVP

Registrar eventos basicos:

- form submitted
- job started
- job completed
- job failed
- report viewed/downloaded

Esto ayuda a medir uso real y preparar la landing/demo.

## Entregables

- Interfaz web MVP funcional
- Formulario de analisis con datos enriquecidos
- Vista de progreso (SSE/polling)
- Vista de resultado/informe
- Manejo basico de errores y reintentos

## Riesgos y mitigaciones

- **Frontend demasiado ambicioso**: limitar a flujo lineal de analisis.
- **Acoplar UI a payloads inestables**: usar contratos de respuesta versionados.
- **Progreso confuso**: reutilizar eventos de job ya definidos en backend.

## Criterios de salida

- Un usuario no tecnico puede lanzar un analisis y ver progreso.
- Puede consumir el resultado sin usar curl ni scripts.
- La UI soporta el flujo de demo comercial de la siguiente fase.

## Dependencia con la siguiente fase

La Fase 9 reutiliza este flujo para una landing de captacion y para enviar un mini resumen de analisis por email.
