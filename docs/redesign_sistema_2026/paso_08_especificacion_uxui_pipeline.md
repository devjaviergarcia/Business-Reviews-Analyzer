# Paso 08 - Especificacion UXUI de Pipeline

## Objetivo UX

Mostrar pipeline de scraping/analisis como un grafo de nodos claro, accionable y en tiempo real.

## Direccion visual

Base cromatica:

- Negro (fondo, profundidad).
- Gris (superficies, separadores, texto secundario).
- Azul (acento y acciones).

Estados obligatorios:

- Exito: Verde.
- Esperando: Naranja.
- Fallo: Rojo.
- Pausado / Intervencion humana: Azul.

## Paleta recomendada

- `--bg-main: #0B0F14`
- `--bg-panel: #121821`
- `--bg-elevated: #1A2330`
- `--border-subtle: #2A3647`
- `--text-primary: #E6EDF5`
- `--text-secondary: #9FB0C3`
- `--accent-blue: #2F6FEB`
- `--state-success: #2EA043`
- `--state-waiting: #D29922`
- `--state-failed: #F85149`
- `--state-human: #2F6FEB`

## Distribucion de pantalla

1. Barra superior

- estado global de TripAdvisor;
- ultima intervencion;
- expiracion de cookie;
- semaforo visual.

2. Columna izquierda

- lista de workflows/jobs;
- filtros por estado/fuente.

3. Canvas central

- nodos conectados por flechas;
- estado en tiempo real.

4. Panel de detalle (persiana)

- se despliega desde derecha o desde abajo;
- posicion elegible por boton.

5. Log inferior

- feed de eventos live con timestamp.

## Anatomia del nodo

Elementos obligatorios:

- `title`: nombre del nodo (ej: `TRIPADVISOR SCRAPE`).
- `subtitle`: `job_id` corto.
- `source_badge`: `google_maps` o `tripadvisor`.
- `status_flag` (esquina superior derecha).
- `status_dot` + texto estado.
- `progress_bar`.
- `metrics_row`:
  - intentos;
  - comments;
  - duracion.
- `action_hint` (ej: "Click para detalle").

Separacion interna:

- padding vertical minimo 12px;
- gap interno 8px;
- linea divisoria entre encabezado y metricas.

## Flag de estado (obligatorio)

Ubicacion:

- esquina superior derecha del nodo.

Forma:

- etiqueta rectangular de alto 20px.

Texto:

- `RUNNING`, `DONE`, `FAILED`, `WAITING`, `NEEDS_HUMAN`.

Color de fondo:

- segun estado.

## Interacciones

## On hover sobre nodo

- elevar nodo (`translateY(-2px)`).
- aumentar borde (1px -> 2px).
- mostrar tooltip corto:
  - estado actual;
  - ultima actualizacion;
  - accion sugerida.

Duracion:

- 120ms ease-out.

## On click sobre nodo

Abre panel de detalle tipo persiana.

Modo derecha:

- panel nace pegado al borde derecho;
- entra hacia el centro;
- ancho recomendado 420px desktop.

Modo abajo:

- panel nace desde borde inferior;
- sube hacia el centro;
- altura recomendada 42vh.

Selector de posicion:

- boton toggle en header del panel:
  - `Posicion: Derecha | Abajo`.

Animacion de apertura/cierre:

- curva: `cubic-bezier(0.2, 0.8, 0.2, 1)`;
- duracion apertura: 220ms;
- duracion cierre: 180ms;
- cierre debe replegar y desaparecer (sin salto).

## Contenido del panel persiana

Secciones:

1. Resumen del nodo.
2. Estado y transiciones.
3. Error actual (si existe).
4. Eventos recientes.
5. Acciones:
  - relanzar;
  - marcar manual;
  - abrir output;
  - copiar job_id.

## Conectores entre nodos

- flecha gris por defecto.
- flecha iluminada en azul cuando flujo activo.
- flecha verde al completar.
- flecha naranja cuando espera input humano.
- flecha roja cuando se corta por fallo.

## Reglas visuales de estado

- `pending`: gris.
- `running`: azul acento + glow suave.
- `completed`: verde.
- `failed`: rojo.
- `needs_human`: azul fuerte + icono de mano/usuario.
- `waiting` (no bloqueante): naranja.

## Accesibilidad y uso

- contraste minimo WCAG AA.
- focus visible en teclado para nodos y botones.
- `Enter` abre panel; `Esc` cierra panel.
- labels y tooltips sin depender solo de color.

## Microcopys recomendados

- `NEEDS_HUMAN`: "Requiere intervencion humana para continuar."
- `SESSION_EXPIRED`: "La sesion de TripAdvisor esta expirada."
- `RELAUNCH_READY`: "Listo para relanzar con la configuracion original."

## Checklist UXUI de aceptacion

- nodos muestran flag y metricas completas;
- hover y click responden con animacion definida;
- panel persiana funciona derecha/abajo por toggle;
- colores de estado son consistentes en nodos, flechas y lista lateral;
- errores y acciones se entienden sin abrir logs tecnicos.

