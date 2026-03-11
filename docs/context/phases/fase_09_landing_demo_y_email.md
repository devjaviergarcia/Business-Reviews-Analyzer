# Fase 9 - Landing de demo + captura de lead + email con resumen inicial

## Objetivo

Crear una landing orientada a conversion que permita agendar una demo y capturar datos del negocio, generando un pequeño resumen de analisis para enviar por correo.

Esta fase conecta el producto con adquisicion comercial.

## Valor de negocio de esta fase

- valida interes real del mercado
- genera leads cualificados
- permite demos con valor percibido antes de una llamada
- reduce friccion comercial (el lead ya recibe algo util)

## Alcance de producto

### Flujo objetivo

1. Usuario llega a landing
2. Introduce email y datos del negocio
3. Solicita demo / analisis inicial
4. Sistema lanza un analisis breve (modo demo)
5. Recibe email con resumen y CTA para agendar demo

### Datos minimos a capturar

- email
- nombre del negocio
- ciudad
- pais (si aplica)
- website (opcional)
- telefono (opcional)
- sector/categoria (opcional)
- consentimiento/aceptacion legal (segun necesidad)

## Alcance tecnico

### Componentes necesarios

- landing frontend (separada o dentro del mismo frontend)
- endpoint de captura de lead
- persistencia de leads
- disparo de job demo (analysis mode = `demo`)
- generacion de mini resumen
- envio de email
- tracking basico de conversion

### Recomendacion de enfoque

- primero landing + captura + persistencia + confirmacion
- despues automatizar demo summary por email
- mantener limite de coste por lead (modo `demo` + muestras cortas)

## Paso a paso (orden recomendado)

### Paso 1 - Disenar propuesta de valor de la landing

Definir claramente:

- que problema resuelves
- para quien
- que recibe el usuario al dejar sus datos
- CTA principal (agendar demo / recibir mini analisis)

No hacer una landing generica; enfocar a reputacion y mejora operativa del negocio.

### Paso 2 - Implementar formulario de captacion

1. Diseñar formulario corto (conversion alta).
2. Validar campos minimos.
3. Guardar lead en backend.
4. Confirmar recepcion al usuario.

Definir estado del lead:

- `new`
- `processing_demo`
- `demo_sent`
- `demo_failed`
- `booked`

### Paso 3 - Conectar con pipeline de analisis demo

1. Crear endpoint/handler que cree un job demo para el lead.
2. Reutilizar modo `demo` definido en Fase 6.
3. Limitar coste/tiempo (fuentes, muestra, profundidad).
4. Persistir relacion `lead -> job -> analysis_run`.

### Paso 4 - Construir mini resumen para email

Definir formato de salida corto (no PDF completo):

- resumen ejecutivo (3-5 lineas)
- 2-3 puntos fuertes
- 2-3 oportunidades de mejora
- CTA para ver demo completa / agendar llamada

Este resumen debe ser util pero dejar espacio para la demo comercial.

### Paso 5 - Envio de email

1. Integrar proveedor de email transaccional.
2. Crear plantilla HTML/texto.
3. Enviar resumen con CTA.
4. Registrar estado de entrega/error.

### Paso 6 - Tracking de conversion

Registrar eventos:

- visit
- submit lead
- demo summary generated
- email sent
- email opened (si proveedor lo permite)
- CTA clicked
- demo booked

Esto permite optimizar la landing y el funnel.

### Paso 7 - Operacion y limites de coste

Definir reglas para evitar abuso:

- rate limit por IP/email
- max jobs demo por periodo
- colas separadas para demos comerciales vs jobs internos
- fallback si scraping falla (enviar mensaje honesto + CTA)

## Entregables

- Landing page de captacion
- Backend de leads + estados
- Integracion con analisis `demo`
- Email con mini resumen de analisis
- Tracking basico de conversion

## Riesgos y mitigaciones

- **Coste por lead demasiado alto**: modo demo muy controlado y timeouts.
- **Fallo de scraping en lead frio**: fallback de email + propuesta de demo manual.
- **Baja conversion por formulario largo**: pedir pocos campos y enriquecer luego.
- **Riesgo legal/compliance**: textos de consentimiento y tratamiento de datos.

## Criterios de salida

- Un lead puede dejar sus datos y recibir un resumen por email.
- El flujo esta instrumentado y se mide conversion basica.
- El sistema soporta demos comerciales reales con un coste controlado.

## Siguiente evolucion natural (post Fase 9)

- panel comercial de leads y demos
- version "self-serve" del producto
- automatizacion de seguimiento
- escalado de workers y observabilidad completa
