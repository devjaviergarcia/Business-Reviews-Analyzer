# Benchmark, Competidores y Deep Study: Casos de Uso

## Para Que Existe Esto

Este modulo convierte el scraper/discovery de Google Maps en una base de producto comercial.

Antes el sistema encontraba negocios y los metia en CRM. Ahora puede crear un estudio local: capturar un mercado concreto, guardar snapshots de negocios, comparar cada negocio con competidores y producir un diagnostico estructurado reutilizable para informes.

Ejemplo base:

```bash
python3 scripts/run_benchmark_study.py \
  --query "restaurantes cordoba" \
  --city "Cordoba" \
  --limit 100 \
  --xvfb
```

Ese comando puede crear la base de un estudio como:

> "Analisis de reputacion y presencia digital de restaurantes en Cordoba."

## Conceptos

### Benchmark

Un benchmark es un estudio local de un sector y ciudad.

Ejemplos:

- `restaurantes cordoba`
- `merienda cordoba`
- `clinicas dentales sevilla`
- `hoteles granada`

Guarda una ejecucion en `benchmark_runs`.

Sirve para responder:

- cuantos negocios se analizaron
- que query se uso
- que ciudad o categoria se estudio
- cuantos negocios se insertaron/actualizaron
- si el scraping fue completo, parcial o fallido

### Benchmark Business

Un benchmark business es cada negocio capturado dentro de un benchmark.

Guarda datos en `benchmark_businesses`:

- nombre
- categoria
- ciudad
- direccion
- rating
- numero de resenas
- telefono
- web
- URL de Google Maps
- posicion de aparicion en el benchmark (`discovery_rank`)
- snapshot crudo
- scores base
- si el listing fue enriquecido

Sirve para tener una foto congelada del negocio en ese estudio.

### Competitor Set

Un competitor set es la lista de competidores seleccionados para un negocio concreto.

Guarda datos en `competitor_sets`.

El selector intenta escoger:

- lideres locales
- negocios similares
- negocios aspiracionales

Usa:

- misma ciudad
- categoria igual o parecida
- posicion en la que aparecio en discovery
- rating
- volumen de resenas
- orden estable y determinista

### Deep Study Snapshot

Un deep study snapshot es el diagnostico estructurado de un negocio.

Usa:

- datos del listing
- posicion en discovery
- reviews, si existen
- competidores
- medias del benchmark, si existen

Devuelve JSON con:

- resumen ejecutivo
- fortalezas
- riesgos
- temas recurrentes
- gaps contra competidores
- acciones mensuales
- plantillas de respuesta
- score breakdown
- warnings de datos incompletos

No depende del HTML. Esto es importante: sirve aunque luego cambie el frontend o el template del informe.

## Diferencia Rapida

| Pieza | Que Es | Para Que Sirve |
|---|---|---|
| Benchmark | Estudio de un mercado local | Capturar 50-100 negocios de un nicho |
| Benchmark business | Un negocio dentro del estudio | Guardar su ficha enriquecida |
| Competitor set | Competidores de un negocio | Comparativa local individual |
| Deep study | Diagnostico estructurado | Base del lead report y paid report |
| Lead report | Informe gratuito corto | Conseguir opt-in/interes |
| Paid report | Informe mensual completo | Producto recurrente de pago |

## Casos De Uso Principales

### 1. Estudio Publico Local

Objetivo: publicar contenido atractivo y compartible.

Ejemplo:

> "Analizamos 100 restaurantes de Cordoba: quien destaca en reputacion, visibilidad y conversion."

Flujo:

1. Lanzar benchmark.
2. Capturar negocios y listings.
3. Agregar medias anonimas.
4. Publicar insights generales.
5. Incluir CTA para pedir informe individual.

Valor:

- genera autoridad
- da contenido para LinkedIn/SEO
- no requiere email frio
- permite captar leads por curiosidad real

### 2. Lead Report Gratuito

Objetivo: que un negocio pida su diagnostico.

Entrada:

- negocio seleccionado del benchmark
- consentimiento del formulario
- competidores ya calculados
- deep study snapshot

Salida:

Un informe corto con:

- score general
- posicion frente al entorno
- 3 oportunidades principales
- 1 comparativa ligera
- 1 accion inmediata
- CTA al radar mensual

Valor:

- convierte trafico organico en lead
- evita vender desde frio
- demuestra utilidad rapido

### 3. Paid Report Mensual

Objetivo: producto recurrente.

Entrada:

- negocio
- reviews actualizadas
- benchmark historico
- competidores
- deep study completo

Salida:

Informe mensual con:

- evolucion
- comparativa con 5 competidores
- riesgos prioritarios
- temas positivos/negativos
- acciones del mes
- plantillas de respuesta
- checklist operativo

Valor:

- justifica recurrencia
- no es un PDF puntual
- vende seguimiento y mejora continua

### 4. Radar De Competidores

Objetivo: mostrar al cliente como se mueve su entorno.

Preguntas que puede responder:

- quien tiene mejor rating
- quien tiene mas resenas
- quien tiene web visible
- quien aparece antes en el benchmark
- que negocios parecen lideres
- quien esta captando mas confianza social

Valor:

- convierte el producto en vigilancia mensual
- da razones para renovar
- crea urgencia sin inventarla

### 5. Priorizacion Comercial En CRM

Objetivo: decidir a quien merece la pena dedicar esfuerzo.

Con los datos del benchmark puedes priorizar negocios con:

- buen rating pero sin web
- muchas resenas pero mala conversion
- bajo volumen de resenas
- aparece tarde en discovery aunque tenga buen rating
- categoria competitiva
- telefono visible pero sin canal digital claro
- oportunidad alta frente a competidores

Valor:

- reduce ruido en discovery
- permite trabajar primero los leads con mas dolor visible

### 6. Segmentacion De Mensajes Y CTAs

Objetivo: personalizar landing o informe sin hacer copy manual.

Ejemplos:

- Si no tiene web: CTA sobre conversion desde Google Maps.
- Si tiene pocas resenas: CTA sobre captacion de prueba social.
- Si rating bajo: CTA sobre gestion de reputacion.
- Si competidores superan en volumen: CTA sobre brecha competitiva.
- Si aparece tarde en discovery: CTA sobre visibilidad local.

Valor:

- mensajes mas concretos
- menos generico
- mejor conversion

### 7. Contenido Para LinkedIn

Objetivo: transformar datos en piezas publicables.

Ejemplos de posts:

- "El 38% de restaurantes analizados no tiene web visible desde Google Maps."
- "Los negocios con mas de 300 resenas no siempre tienen mejor rating, pero si mas confianza percibida."
- "Que separa a los lideres locales de los negocios invisibles en Maps."

Valor:

- contenido propio
- no dependes de opiniones vagas
- cada post puede llevar a CTA del informe

### 8. SEO Local Y Paginas De Estudio

Objetivo: captar busquedas informacionales.

Paginas posibles:

- `/estudios/restaurantes-cordoba-2026`
- `/estudios/meriendas-cordoba-google-maps`
- `/comparativas/reputacion-restaurantes-cordoba`

Valor:

- activos permanentes
- captacion organica
- enlaza con formulario de informe individual

### 9. Diagnostico Sin Reviews Todavia

Objetivo: no bloquear el flujo si solo hay listing.

El deep study degrada bien si faltan reviews.

Puede seguir usando:

- rating
- numero de resenas
- telefono
- web
- categoria
- direccion
- competidores

Valor:

- puedes generar un primer diagnostico rapido
- luego enriquecer con reviews para paid report

### 10. Comparativa Antes/Despues

Objetivo: medir progreso mensual.

Si guardas benchmarks sucesivos puedes comparar:

- rating anterior vs actual
- resenas anteriores vs actuales
- posicion anterior vs actual en discovery
- web/telefono/listing actualizado
- score de oportunidad
- posicion relativa frente a competidores

Valor:

- convierte el producto en seguimiento
- permite demostrar mejora

## Flujo Ideal De Producto

```text
Benchmark local
  -> benchmark_businesses
  -> competitor_sets
  -> deep_study_snapshot
  -> lead_report gratuito
  -> paid_report mensual
  -> seguimiento mensual
```

## Flujo Ideal De Captacion

```text
Post / SEO / Comunidad / QR
  -> Landing del estudio
  -> CTA "pide tu diagnostico"
  -> Consentimiento explicito
  -> Cola local genera informe
  -> Email transaccional con informe solicitado
  -> CTA a radar mensual
```

## Que Esta Implementado Ya

### Implementado

- Modelos Mongo para benchmark.
- Repositorios Mongo para benchmark.
- Job `benchmark_local_study`.
- Worker CRM compatible con el job.
- Script `scripts/run_benchmark_study.py`.
- Orquestador de benchmark.
- Persistencia de negocios del benchmark.
- Selector determinista de competidores.
- Persistencia de `competitor_sets`.
- Builder `build_deep_study_snapshot(...)`.
- Tests de repositorios, contratos, orquestador, competidores y deep study.

### No Implementado Todavia

- Pantalla UI para lanzar benchmark.
- Pantalla UI para explorar benchmark.
- Endpoint publico de solicitud de informe.
- Lead report HTML final.
- Paid report HTML final.
- Estudio publico HTML.
- Automatizacion completa de cola local al iniciar PC.
- Tracking completo de UTMs/canales en formularios.

## Comandos Utiles

### Ejecutar Benchmark Directo

```bash
python3 scripts/run_benchmark_study.py \
  --query "restaurantes cordoba" \
  --city "Cordoba" \
  --limit 100 \
  --xvfb
```

### Encolar Benchmark Para Worker

```bash
python3 scripts/run_benchmark_study.py \
  --query "restaurantes cordoba" \
  --city "Cordoba" \
  --limit 100 \
  --enqueue
```

### Ver Tareas Del Plan

```bash
python3 scripts/tasks.py list
```

### Ver Siguiente Tarea

```bash
python3 scripts/tasks.py show T05
```

## Como Pensarlo Comercialmente

Esto no es solo scraping.

Es una cadena de valor:

1. Datos publicos estructurados.
2. Comparativa local.
3. Diagnostico individual.
4. Informe gratuito bajo opt-in.
5. Radar mensual de pago.

La parte importante no es decirle al negocio "tienes 4,6 estrellas".

La parte importante es decirle:

> "Tienes 4,6, pero tus competidores directos tienen mas volumen, mejor conversion desde Maps y mas senales de confianza. Estas son las 3 acciones de este mes para cerrar esa brecha."

## Siguiente Paso Natural

La siguiente tarea es `T05 - Lead report V1`.

Objetivo:

Convertir `deep_study_snapshot` en un informe corto, claro y vendible.

Debe poder generarse para un negocio del benchmark y servir como primer activo que el usuario pide desde una landing.
