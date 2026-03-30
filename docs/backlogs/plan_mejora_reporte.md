# Plan de mejora del reporte HTML — Business Reviews Analyzer
> Versión 1.0 · Marzo 2026

---

## Índice

1. [Resumen ejecutivo](#1-resumen-ejecutivo)
2. [Problemas de contenido](#2-problemas-de-contenido)
3. [Problemas de estructura](#3-problemas-de-estructura)
4. [Problemas de diseño](#4-problemas-de-diseño)
5. [Orden de implementación](#5-orden-de-implementación)
6. [Checklist de verificación final](#6-checklist-de-verificación-final)

---

## 1. Resumen ejecutivo

El reporte actual tiene tres capas de problema que se acumulan: **datos técnicos internos visibles al cliente**, **estructura sin jerarquía clara entre lo positivo y lo negativo**, y **diseño sin diferenciación visual de urgencia**. Ninguno de estos problemas requiere cambios en la lógica de análisis — todos son de presentación y post-procesado.

**Tiempo estimado total de implementación: 2-3 jornadas de trabajo.**

| Capa | Problemas | Impacto percibido | Dificultad |
|---|---|---|---|
| Contenido | 7 | Alto | Baja |
| Estructura | 4 | Alto | Media |
| Diseño | 4 | Medio | Media |

---

## 2. Problemas de contenido

### PC-01 — Sección "Introducción" con datos técnicos internos

**Problema actual:**
La primera sección visible del reporte muestra IDs de MongoDB, timestamps ISO y ventanas temporales en formato máquina. Es lo primero que ve el cliente y destruye la credibilidad del informe.

```
# Lo que aparece ahora:
"pertenecientes al conjunto de reseñas con identificador 69c7cfa1a79fed2455c34d73.
Ventana temporal observada: 2026-03-28T12:49:22.242000 -> 2026-03-28T12:49:22.242000"
```

**Solución:**
Eliminar completamente la sección de introducción técnica y reemplazarla por un banner de contexto limpio.

**Implementación — `advanced_report_builder.py` (sección HTML renderer):**

```python
# ANTES — en el método que genera el HTML de la sección intro:
def _render_intro_section(self, dataset_id, review_count, sources, window):
    return f"""
    <section class='intro'>
      <h2>Introducción: de dónde salen estas reseñas</h2>
      <p>...identificador {dataset_id}. Ventana temporal: {window}...</p>
    </section>
    """

# DESPUÉS — banner limpio sin datos internos:
def _render_intro_section(self, review_count, sources, generated_at):
    date_str = _format_date_human(generated_at)  # ver función auxiliar abajo
    sources_str = ", ".join(f"{src}: {count}" for src, count in sources.items())
    return f"""
    <section class='intro context-banner'>
      <div class='context-row'>
        <span class='context-item'>📊 <strong>{review_count}</strong> opiniones analizadas</span>
        <span class='context-item'>📍 Fuentes: <strong>{sources_str}</strong></span>
        <span class='context-item'>🗓️ Actualizado: <strong>{date_str}</strong></span>
      </div>
    </section>
    """

# Función auxiliar de formato de fecha:
from datetime import datetime

def _format_date_human(iso_string: str) -> str:
    """Convierte '2026-03-28T12:56:31.254664+00:00' en '28 de marzo de 2026'"""
    try:
        dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
        months = ["enero","febrero","marzo","abril","mayo","junio",
                  "julio","agosto","septiembre","octubre","noviembre","diciembre"]
        return f"{dt.day} de {months[dt.month - 1]} de {dt.year}"
    except Exception:
        return iso_string[:10]
```

**CSS a añadir:**
```css
.context-banner {
  background: linear-gradient(135deg, var(--accent-1) 0%, #fff 100%);
  padding: 12px 20px;
}
.context-row {
  display: flex;
  flex-wrap: wrap;
  gap: 20px;
  align-items: center;
}
.context-item {
  font-size: 13px;
  color: var(--muted);
}
.context-item strong {
  color: var(--text);
}
```

---

### PC-02 — "Aciertos" como citas literales sin abstraer

**Problema actual:**
Los aciertos son citas sin procesar: *"Tienen la mejor lasaña del mundo!!!"*. No transmiten un punto fuerte estratégico, transmiten una anécdota.

**Solución:**
Dos niveles: titular abstracto (el punto fuerte) + cita literal como evidencia debajo.

**Implementación — prompt de la LLM en `_build_llm_section_narratives()`:**

Añadir al payload de la LLM un campo específico:

```python
payload["instruccion_aciertos"] = (
    "Para cada cita positiva, extrae el CONCEPTO estratégico que representa, "
    "no la cita en sí. Ejemplo: 'Tienen la mejor lasaña del mundo' → "
    "concepto: 'Platos icónicos con identidad propia'. "
    "La cita queda como evidencia, el concepto es el titular."
)
```

Y en el renderer HTML, cambiar la estructura de los aciertos:

```python
# ANTES:
def _render_aciertos(self, positive_quotes):
    items = "".join(f"<li>{q}</li>" for q in positive_quotes[:3])
    return f"<ul>{items}</ul>"

# DESPUÉS:
def _render_aciertos(self, strengths_with_quotes):
    # strengths_with_quotes: [{"concepto": "...", "cita": "..."}]
    cards = ""
    for item in strengths_with_quotes[:3]:
        cards += f"""
        <article class='strength-card'>
          <div class='strength-title'>✓ {item['concepto']}</div>
          <div class='strength-quote'>"{item['cita']}"</div>
        </article>
        """
    return f"<div class='strength-grid'>{cards}</div>"
```

**CSS:**
```css
.strength-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 8px;
  margin-top: 8px;
}
.strength-card {
  border-left: 3px solid var(--good);
  background: #f0fdf6;
  border-radius: 0 10px 10px 0;
  padding: 10px 12px;
}
.strength-title {
  font-weight: 600;
  font-size: 13px;
  color: var(--good);
  margin-bottom: 4px;
}
.strength-quote {
  font-size: 12px;
  color: var(--muted);
  font-style: italic;
}
```

---

### PC-03 — Texto redundante en sección de puntuación

**Problema actual:**
- La palabra "reputación" aparece duplicada: *"puntuación de reputación de reputación"*
- Los mismos datos (72.1, 4.55, 0%) aparecen en las pills de arriba y luego de nuevo en las metric-cards

**Solución:**
Limpieza en dos pasos.

**Implementación:**

1. **Fix del typo en `_plainify_business_text()`** — añadir al diccionario de reemplazos:

```python
# En la lista de replacements de _plainify_business_text():
("puntuación de reputación de reputación", "puntuación de reputación"),
("reputación de reputación", "reputación"),
```

2. **Eliminar las metric-cards que duplican las pills** — en el renderer HTML, no mostrar avg_rating ni response_rate en las metric-cards si ya aparecen en las pills del resumen. Solo mostrar en metric-cards los indicadores que NO están en las pills:

```python
# Pills muestran: score, nivel, cluster_count, problema_count
# Metric-cards solo muestran: sentiment, tranquility, negative_ratio (datos que no están en pills)
METRIC_CARDS_WHITELIST = {"sentiment", "tranquility_aggressiveness", "negative_ratio"}
```

---

### PC-04 — IDs internos en la tabla de histó rico

**Problema actual:**
La tabla de evolución muestra `Analysis id: 69c7cfffeb27315442333a95` — un ObjectId de MongoDB.

**Solución:**
Mostrar solo la fecha formateada. El ID no aporta nada al cliente.

**Implementación — renderer de la tabla de histó rico:**

```python
# ANTES:
def _render_history_row(self, analysis):
    return f"""
    <tr>
      <th>Analysis id</th><td>{analysis['analysis_id']}</td>
    </tr>
    <tr>
      <th>Sentimiento global</th><td>{analysis['overall_sentiment']}</td>
    </tr>
    <tr>
      <th>Fecha</th><td>{analysis['created_at']}</td>
    </tr>
    """

# DESPUÉS — tabla limpia:
def _render_history_row(self, analysis):
    sentiment_label = {
        "positive": "✓ Positivo",
        "mixed": "~ Mixto",
        "negative": "✗ Negativo"
    }.get(analysis['overall_sentiment'], analysis['overall_sentiment'])
    
    date_str = _format_date_human(str(analysis.get('created_at', '')))
    return f"""
    <tr>
      <th>{date_str}</th>
      <td>{sentiment_label}</td>
    </tr>
    """
```

---

### PC-05 — Typos generados por la LLM ("impactoo", "impactoo")

**Problema actual:**
La LLM a veces genera caracteres duplicados: *"tiene impactoo directo"*.

**Solución:**
Post-procesado sistemático de todo texto generado por la LLM antes de renderizar.

**Implementación — añadir función en `advanced_report_builder.py`:**

```python
import re

def _sanitize_llm_text(self, text: str) -> str:
    """
    Limpia artefactos comunes de generación LLM antes de mostrar al cliente.
    Llamar sobre cualquier string que venga de la LLM antes de renderizarlo.
    """
    if not text:
        return ""
    
    # Caracteres de letra duplicados al final de palabra (impactoo → impacto)
    # Patrón: misma letra vocal/consonante repetida 2+ veces al final o medio de palabra
    text = re.sub(r'([a-záéíóúüñ])\1{2,}', r'\1\1', text, flags=re.IGNORECASE)
    text = re.sub(r'([aeiouáéíóúü])\1+', r'\1', text, flags=re.IGNORECASE)
    
    # Puntos dobles o triples
    text = re.sub(r'\.{2,}', '.', text)
    
    # Espacios múltiples
    text = re.sub(r' {2,}', ' ', text)
    
    # Mayúsculas pegadas sin espacio (artefacto de tokenización)
    text = re.sub(r'([a-záéíóúü])([A-ZÁÉÍÓÚÜ])', r'\1 \2', text)
    
    return text.strip()
```

**Dónde llamarla:**
```python
# En _build_llm_section_narratives(), antes de devolver:
for key in merged:
    merged[key] = self._sanitize_llm_text(merged[key])

# En _build_llm_action_plan(), sobre cada campo de texto:
accion_text = self._sanitize_llm_text(str(item.get("accion", "")))
```

---

### PC-06 — Celda "Puntuación del negocio" vacía en tabla de competidores

**Problema actual:**
La tabla de benchmarking tiene una fila con la celda vacía porque el campo no llega al renderer.

**Solución:**
Condicional antes de renderizar + valor de fallback.

**Implementación:**
```python
# En el renderer de la tabla de benchmarking:
score_value = benchmarking_summary.get("target_reputation_score")
score_display = f"{round(float(score_value), 1)}/100" if score_value else "—"

# En el HTML:
f"<td>{score_display}</td>"
```

---

### PC-07 — Footer "generado automáticamente" quita valor

**Problema actual:**
*"Reporte generado automáticamente por Business Review Analyzer."* — comunica que no hay trabajo humano detrás.

**Solución:**
Cambiar el mensaje y añadir branding mínimo.

**Implementación:**
```python
# ANTES:
footer = "Reporte generado automáticamente por Business Review Analyzer."

# DESPUÉS:
footer = f"Análisis elaborado por Business Reviews Analyzer · {date_str}"
```

---

## 3. Problemas de estructura

### PE-01 — Sin separación visual entre puntos fuertes y débiles

**Problema actual:**
Todo el contenido está en secciones blancas genéricas. El cliente no sabe de un vistazo qué funciona bien y qué hay que mejorar.

**Solución:**
Añadir dos bloques diferenciados visualmente antes del plan de acción: **Lo que funciona** (verde) y **Lo que hay que mejorar** (ámbar/rojo).

**Implementación — nueva sección en el renderer:**

```python
def _render_strengths_weaknesses_section(
    self,
    strengths: list[dict],
    weaknesses: list[dict]
) -> str:
    """
    Genera el bloque visual de puntos fuertes y débiles.
    strengths: lista de {"titulo": str, "descripcion": str, "como_mantener": str}
    weaknesses: lista de {"titulo": str, "descripcion": str, "tipo": str}
    """
    # Puntos fuertes
    strong_cards = ""
    for item in strengths[:4]:
        strong_cards += f"""
        <article class='fw-card fw-strong'>
          <div class='fw-icon'>✓</div>
          <div class='fw-body'>
            <div class='fw-title'>{item['titulo']}</div>
            <div class='fw-desc'>{item['descripcion']}</div>
            <div class='fw-action'>
              <span class='fw-label'>Cómo mantenerlo:</span> {item.get('como_mantener', '')}
            </div>
          </div>
        </article>
        """

    # Puntos débiles
    weak_cards = ""
    tipo_icons = {
        "proceso": "⚙️",
        "negocio": "💼",
        "implementacion": "🔧",
        "tecnologico": "📱",
    }
    for item in weaknesses[:4]:
        tipo = item.get('tipo', 'proceso')
        icon = tipo_icons.get(tipo, "⚙️")
        weak_cards += f"""
        <article class='fw-card fw-weak'>
          <div class='fw-icon'>✗</div>
          <div class='fw-body'>
            <div class='fw-title'>{item['titulo']}</div>
            <div class='fw-desc'>{item['descripcion']}</div>
            <div class='fw-meta'>
              <span class='fw-tipo-badge'>{icon} {tipo.capitalize()}</span>
            </div>
          </div>
        </article>
        """

    return f"""
    <section class='section'>
      <h2>Qué funciona bien y qué hay que mejorar</h2>
      <div class='fw-grid'>
        <div class='fw-col'>
          <h3 class='fw-col-title fw-col-strong'>✓ Puntos fuertes</h3>
          {strong_cards}
        </div>
        <div class='fw-col'>
          <h3 class='fw-col-title fw-col-weak'>✗ Puntos a mejorar</h3>
          {weak_cards}
        </div>
      </div>
    </section>
    """
```

**CSS:**
```css
.fw-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  margin-top: 12px;
}
@media (max-width: 720px) {
  .fw-grid { grid-template-columns: 1fr; }
}
.fw-col-title {
  font-size: 14px;
  font-weight: 700;
  margin: 0 0 10px 0;
  padding-bottom: 6px;
  border-bottom: 2px solid currentColor;
}
.fw-col-strong { color: var(--good); }
.fw-col-weak { color: var(--warn); }
.fw-card {
  display: flex;
  gap: 10px;
  border-radius: 10px;
  padding: 10px 12px;
  margin-bottom: 8px;
}
.fw-strong {
  background: #f0fdf6;
  border: 1px solid #c5f1d7;
}
.fw-weak {
  background: #fff8f0;
  border: 1px solid #ffd9af;
}
.fw-icon {
  font-size: 16px;
  font-weight: 800;
  flex-shrink: 0;
  margin-top: 2px;
}
.fw-strong .fw-icon { color: var(--good); }
.fw-weak .fw-icon { color: var(--warn); }
.fw-title {
  font-weight: 600;
  font-size: 13px;
  margin-bottom: 3px;
}
.fw-desc {
  font-size: 12px;
  color: var(--muted);
  margin-bottom: 6px;
}
.fw-action {
  font-size: 12px;
  color: var(--text);
}
.fw-label {
  font-weight: 600;
}
.fw-tipo-badge {
  display: inline-block;
  font-size: 11px;
  background: #fff3e4;
  color: #aa5f0e;
  border: 1px solid #ffd9af;
  border-radius: 999px;
  padding: 2px 8px;
}
```

**Qué datos alimentan esta sección:**
Esta sección consume el output del agente de IA del sistema prompt que generamos en la sesión anterior (`puntos_fuertes` y `puntos_debiles` del JSON). Por tanto, depende de que el agente esté integrado en el pipeline antes del renderer.

---

### PE-02 — Plan de acción sin etiqueta de tipo de solución

**Problema actual:**
Las action-cards no indican si la acción es de proceso, de negocio, tecnológica o de implementación. El cliente no sabe qué tipo de esfuerzo implica.

**Solución:**
Añadir un badge de tipo en cada action-card.

**Implementación — renderer de action-cards:**

```python
TIPO_CONFIG = {
    "proceso":        {"label": "Proceso interno",  "color": "#e3f0ff", "text": "#1a5fa8", "border": "#b3d1f5"},
    "negocio":        {"label": "Decisión de negocio", "color": "#fdf0e3", "text": "#a85f1a", "border": "#f5d1b3"},
    "implementacion": {"label": "Implementación",   "color": "#f3e3ff", "text": "#7a1aa8", "border": "#d9b3f5"},
    "tecnologico":    {"label": "Solución tecnológica", "color": "#e3fff0", "text": "#1aa85f", "border": "#b3f5d1"},
}

def _render_action_card(self, action: dict) -> str:
    tipo = action.get("tipo", "proceso")
    cfg = TIPO_CONFIG.get(tipo, TIPO_CONFIG["proceso"])
    badge_html = f"""
    <span class='tipo-badge' style='background:{cfg["color"]};color:{cfg["text"]};
          border-color:{cfg["border"]}'>
      {cfg["label"]}
    </span>
    """
    herramienta = action.get("herramienta_si_aplica", "")
    herramienta_html = (
        f"<div class='meta-line'>🔧 Herramienta: {herramienta}</div>"
        if herramienta else ""
    )
    return f"""
    <li class='action-card'>
      <div class='action-card-header'>
        <div class='title'>{action.get('accion', action.get('action', ''))}</div>
        {badge_html}
      </div>
      <div class='meta-line'>👤 {action.get('encargado', action.get('owner', ''))}</div>
      <div class='meta-line'>📅 Plazo: {action.get('plazo_dias', action.get('horizon_days', ''))} días</div>
      <div class='meta-line'>📏 {action.get('indicador', action.get('kpi', ''))}</div>
      {herramienta_html}
    </li>
    """
```

**CSS:**
```css
.action-card-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 8px;
  margin-bottom: 6px;
}
.tipo-badge {
  display: inline-block;
  font-size: 10px;
  font-weight: 600;
  border-radius: 999px;
  border: 1px solid;
  padding: 2px 8px;
  white-space: nowrap;
  flex-shrink: 0;
}
```

---

### PE-03 — "Acciones rápidas" duplica el plan de acción

**Problema actual:**
Las mismas acciones aparecen en el timeline (corto/medio/largo) y luego de nuevo en la sección "Acciones rápidas esta semana". El cliente siente que el reporte está inflado.

**Solución:**
Fusionar ambas secciones. Las "acciones rápidas" pasan a ser la cabecera destacada del plan, no una sección separada al final.

**Implementación — reestructurar el orden del renderer:**

```python
def _render_action_plan_section(self, action_plan, quick_wins, invisible_problems):
    # 1. Banner de acción urgente (lo que antes eran "acciones rápidas")
    urgent_html = self._render_urgent_actions(quick_wins)
    
    # 2. Problemas invisibles
    invisible_html = self._render_invisible_problems(invisible_problems)
    
    # 3. Timeline corto/medio/largo (sin repetir las urgentes)
    timeline_html = self._render_timeline(action_plan)
    
    return f"""
    <section class='section'>
      <h2>Plan de acción</h2>
      <p>{action_plan.get('lectura_ejecutiva', '')}</p>
      {urgent_html}
      {invisible_html}
      {timeline_html}
    </section>
    """

def _render_urgent_actions(self, quick_wins):
    items = quick_wins.get("items", []) if quick_wins else []
    if not items:
        return ""
    cards = "".join(self._render_quick_win_card(item) for item in items[:4])
    return f"""
    <div class='urgent-block'>
      <h3 class='urgent-title'>⚡ Esta semana — acciones de impacto inmediato</h3>
      <ul class='action-list'>{cards}</ul>
    </div>
    """
```

**CSS:**
```css
.urgent-block {
  background: linear-gradient(135deg, #fff8f0 0%, #fff 100%);
  border: 1px solid #ffd9af;
  border-left: 4px solid var(--warn);
  border-radius: 0 12px 12px 0;
  padding: 12px 14px;
  margin: 12px 0;
}
.urgent-title {
  color: #aa5f0e;
  font-size: 14px;
  margin: 0 0 8px 0;
}
```

---

### PE-04 — Anexo técnico expuesto directamente

**Problema actual:**
Los indicadores técnicos (sentimiento 0.69, brecha 0.02, intención de mejora 0.01) están en el cuerpo principal del reporte. Para el dueño de un negocio local son números sin contexto.

**Solución:**
Colapsar el anexo en un `<details>` / `<summary>` desplegable. Visible si el cliente quiere profundizar, invisible por defecto.

**Implementación:**

```python
def _render_annex_section(self, annex_data):
    inner_html = self._render_annex_inner(annex_data)
    return f"""
    <section class='section'>
      <details class='annex-details'>
        <summary class='annex-summary'>
          📎 Datos técnicos del análisis <span class='annex-hint'>(despliega para ver)</span>
        </summary>
        <div class='annex-body'>
          {inner_html}
        </div>
      </details>
    </section>
    """
```

**CSS:**
```css
.annex-details {
  cursor: pointer;
}
.annex-summary {
  font-weight: 600;
  font-size: 14px;
  color: var(--muted);
  list-style: none;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 4px 0;
}
.annex-summary::-webkit-details-marker { display: none; }
.annex-hint {
  font-weight: 400;
  font-size: 12px;
}
.annex-details[open] .annex-summary {
  color: var(--text);
  margin-bottom: 12px;
}
.annex-body {
  border-top: 1px solid var(--line);
  padding-top: 12px;
}
```

---

## 4. Problemas de diseño

### PD-01 — Scatter ilegible con puntos apilados

**Problema actual:**
La mayoría de puntos caen en `x ≈ 0` por el cálculo de escala absoluta. El gráfico es correcto pero visualmente inútil.

**Solución:**
Ya documentada en `action_plan_fixes.py` (Fix 4 — escala relativa). Además, añadir un gráfico de barras horizontal como alternativa más legible para el cliente no técnico.

**Implementación — gráfico de barras de tipos de cliente:**

```python
def _render_customer_bar_chart(self, customer_clusters: dict) -> str:
    """
    Genera un gráfico de barras horizontal SVG con el peso de cada tipo de cliente.
    Más legible que el scatter para el dueño del negocio.
    """
    clusters = customer_clusters.get("clusters", [])
    if not clusters:
        return ""

    total_reviews = sum(c.get("count_reviews", 0) for c in clusters) or 1
    COLORS = ["#1b9c5a", "#71EBA1", "#f08b1d", "#cf2f35"]
    BAR_HEIGHT = 32
    GAP = 10
    SVG_WIDTH = 600
    LABEL_WIDTH = 160
    BAR_MAX_WIDTH = SVG_WIDTH - LABEL_WIDTH - 80

    rows = ""
    for i, cluster in enumerate(clusters[:5]):
        label = cluster.get("label", "")[:28]
        count = cluster.get("count_reviews", 0)
        pct = count / total_reviews
        bar_w = round(pct * BAR_MAX_WIDTH)
        y = i * (BAR_HEIGHT + GAP)
        color = COLORS[i % len(COLORS)]
        pct_label = f"{round(pct * 100)}%"
        rows += f"""
        <text x='0' y='{y + BAR_HEIGHT // 2 + 5}' fill='#10312a' font-size='12'
              font-family='Poppins,sans-serif'>{label}</text>
        <rect x='{LABEL_WIDTH}' y='{y}' width='{bar_w}' height='{BAR_HEIGHT}'
              rx='6' fill='{color}' opacity='0.85'/>
        <text x='{LABEL_WIDTH + bar_w + 6}' y='{y + BAR_HEIGHT // 2 + 5}'
              fill='#4b6e67' font-size='11' font-family='Poppins,sans-serif'>
          {pct_label} ({count})
        </text>
        """

    total_height = len(clusters[:5]) * (BAR_HEIGHT + GAP)
    return f"""
    <div class='bar-chart-wrap'>
      <svg viewBox='0 0 {SVG_WIDTH} {total_height}' width='100%' height='{total_height}'>
        {rows}
      </svg>
    </div>
    """
```

Mostrar el scatter y el bar chart en tabs o simplemente mostrar primero el bar chart y el scatter como "vista detallada" colapsada.

---

### PD-02 — Action cards sin diferenciación visual de urgencia

**Problema actual:**
Todas las tarjetas de acción tienen el mismo estilo visual independientemente de si son urgentes, de medio plazo o de largo plazo.

**Solución:**
Color de borde izquierdo según horizonte temporal.

**Implementación — CSS por columna del timeline:**

```css
/* Corto plazo — borde rojo/naranja */
.timeline-col:nth-child(1) .action-card {
  border-left: 3px solid var(--warn);
}

/* Medio plazo — borde azul */
.timeline-col:nth-child(2) .action-card {
  border-left: 3px solid #2e86ab;
}

/* Largo plazo — borde gris */
.timeline-col:nth-child(3) .action-card {
  border-left: 3px solid #8a9ba8;
}

/* Cabecera de cada columna con color propio */
.timeline-col:nth-child(1) h4 { color: var(--warn); }
.timeline-col:nth-child(2) h4 { color: #2e86ab; }
.timeline-col:nth-child(3) h4 { color: #8a9ba8; }
```

---

### PD-03 — Sin jerarquía visual entre secciones

**Problema actual:**
Todas las secciones tienen el mismo peso visual (blanco, borde verde claro, sombra suave). No hay forma de saber qué es más importante.

**Solución:**
Añadir un acento de color lateral distinto por sección para dar identidad visual a cada bloque.

**Implementación — CSS con `border-left` por sección:**

```python
# En el renderer, añadir clase específica a cada sección:
SECTION_CLASSES = {
    "diagnostico":   "section section--diagnostico",
    "puntuacion":    "section section--puntuacion",
    "cliente":       "section section--cliente",
    "fw":            "section section--fw",
    "accion":        "section section--accion",
    "anexo":         "section section--anexo",
}
```

```css
.section--diagnostico  { border-left: 4px solid #1b9c5a; }
.section--puntuacion   { border-left: 4px solid #71E2EB; }
.section--cliente      { border-left: 4px solid #B1F58C; }
.section--fw           { border-left: 4px solid #f08b1d; }
.section--accion       { border-left: 4px solid #cf2f35; }
.section--anexo        { border-left: 4px solid #cccccc; }
```

---

### PD-04 — Score card sin contexto visual de escala

**Problema actual:**
El número `72.1/100` aparece solo. El cliente no sabe si es bueno, regular o malo de un vistazo.

**Solución:**
Añadir una barra de progreso visual debajo del score que muestre dónde cae en la escala 0-100, con zonas de color.

**Implementación:**

```python
def _render_score_card(self, score_value: float, score_label: str) -> str:
    pct = round(min(max(score_value, 0), 100))
    
    # Color del marcador según nivel
    if score_value >= 85:
        marker_color = "#1b9c5a"
    elif score_value >= 70:
        marker_color = "#71EBA1"
    elif score_value >= 55:
        marker_color = "#f08b1d"
    else:
        marker_color = "#cf2f35"

    return f"""
    <div class='score-card'>
      <div class='score-value'>{round(score_value, 1)}/100</div>
      <div class='score-label'>{score_label}</div>
      <div class='score-bar-wrap'>
        <div class='score-bar-track'>
          <div class='score-bar-zones'>
            <div class='zone zone-red'></div>
            <div class='zone zone-orange'></div>
            <div class='zone zone-yellow'></div>
            <div class='zone zone-green'></div>
          </div>
          <div class='score-bar-marker' style='left:{pct}%;background:{marker_color}'></div>
        </div>
        <div class='score-bar-labels'>
          <span>0</span><span>55</span><span>70</span><span>85</span><span>100</span>
        </div>
      </div>
    </div>
    """
```

```css
.score-bar-wrap {
  margin-top: 12px;
}
.score-bar-track {
  position: relative;
  height: 8px;
  border-radius: 4px;
  overflow: visible;
}
.score-bar-zones {
  display: flex;
  height: 100%;
  border-radius: 4px;
  overflow: hidden;
}
.zone { flex: 1; }
.zone-red    { background: #cf2f35; flex: 0.55; }
.zone-orange { background: #f08b1d; flex: 0.15; }
.zone-yellow { background: #f5c842; flex: 0.15; }
.zone-green  { background: #1b9c5a; flex: 0.15; }
.score-bar-marker {
  position: absolute;
  top: -4px;
  width: 16px;
  height: 16px;
  border-radius: 50%;
  border: 2px solid white;
  transform: translateX(-50%);
  box-shadow: 0 2px 6px rgba(0,0,0,0.2);
}
.score-bar-labels {
  display: flex;
  justify-content: space-between;
  font-size: 10px;
  color: var(--muted);
  margin-top: 4px;
}
```

---

## 5. Orden de implementación

Ordenado por impacto/esfuerzo. Hacer en este orden para que cada entrega sea un reporte visiblemente mejor.

### Sprint 1 — "Sin datos internos" (½ jornada)
Impacto inmediato en credibilidad. Cambios solo en el renderer HTML, cero riesgo.

| # | Tarea | Archivo | Tiempo |
|---|---|---|---|
| 1 | PC-01: Eliminar IDs y timestamps de la intro | `report_renderer.py` | 30 min |
| 2 | PC-04: Ocultar Analysis IDs del histó rico | `report_renderer.py` | 15 min |
| 3 | PC-06: Rellenar celda vacía de benchmarking | `report_renderer.py` | 10 min |
| 4 | PC-07: Cambiar texto del footer | `report_renderer.py` | 5 min |

### Sprint 2 — "Sin ruido de texto" (½ jornada)
Limpieza de contenido generado por LLM.

| # | Tarea | Archivo | Tiempo |
|---|---|---|---|
| 5 | PC-05: Añadir `_sanitize_llm_text()` | `advanced_report_builder.py` | 30 min |
| 6 | PC-03: Fix "reputación de reputación" y deduplicar pills | `advanced_report_builder.py` + renderer | 20 min |

### Sprint 3 — "Estructura clara" (1 jornada)
Cambios estructurales que requieren datos del agente de IA.

| # | Tarea | Archivo | Tiempo |
|---|---|---|---|
| 7 | PE-01: Sección puntos fuertes/débiles | renderer + CSS | 2h |
| 8 | PE-03: Fusionar acciones rápidas con timeline | renderer | 1h |
| 9 | PE-04: Colapsar anexo técnico en `<details>` | renderer + CSS | 30 min |
| 10 | PC-02: Abstraer aciertos con concepto + cita | prompt LLM + renderer | 1h |

### Sprint 4 — "Diseño con jerarquía" (1 jornada)
Mejoras visuales puras en CSS y SVG.

| # | Tarea | Archivo | Tiempo |
|---|---|---|---|
| 11 | PE-02: Badge de tipo en action-cards | renderer + CSS | 45 min |
| 12 | PD-02: Bordes de color por horizonte en timeline | CSS | 20 min |
| 13 | PD-03: Acento lateral por sección | renderer + CSS | 30 min |
| 14 | PD-04: Barra de progreso en score card | renderer + CSS | 1h |
| 15 | PD-01: Bar chart de tipos de cliente | renderer SVG | 1.5h |

---

## 6. Checklist de verificación final

Antes de dar por cerrada cada iteración del reporte, verificar punto a punto:

### Contenido
- [ ] No aparece ningún ObjectId ni hash de MongoDB en el HTML visible
- [ ] No aparece ningún timestamp ISO (`2026-03-28T12:49:22`) en el HTML visible
- [ ] Los "aciertos" son conceptos abstractos, no citas literales como titulares
- [ ] No hay texto duplicado (mismos datos en pills y en metric-cards)
- [ ] El histó rico de análisis muestra solo fechas legibles
- [ ] Ningún campo de texto tiene caracteres duplicados (impactoo, etc.)
- [ ] La celda de puntuación en la tabla de competidores tiene valor
- [ ] El footer no dice "generado automáticamente"

### Estructura
- [ ] Hay una sección visualmente diferenciada de puntos fuertes vs débiles
- [ ] Cada acción tiene una etiqueta de tipo (proceso / negocio / implementación / tecnológico)
- [ ] Las "acciones rápidas" no se repiten en la sección del plan de acción
- [ ] El anexo técnico está colapsado por defecto

### Diseño
- [ ] El scatter tiene puntos distribuidos en el rango visible, no apilados en un eje
- [ ] Las action-cards de corto plazo tienen acento visual diferente (naranja)
- [ ] Cada sección principal tiene identidad visual propia (borde lateral de color)
- [ ] El score card tiene barra de progreso con zonas de color
- [ ] El reporte se ve bien en móvil (media query activo)

### Calidad general
- [ ] El reporte se puede imprimir / guardar como PDF sin elementos cortados
- [ ] Todos los textos generados por LLM pasan por `_sanitize_llm_text()` antes de renderizar
- [ ] Los datos de competidores no muestran nombres de otros negocios de la base de datos
- [ ] La fecha del reporte es legible en español
