#!/usr/bin/env python3
"""
Open an interactive map to manually pick geo points for local rank studies.

Examples:
  python3 scripts/pick_city_points.py --center 37.8882,-4.7794 --city "Cordoba"
  python3 scripts/pick_city_points.py --lat 37.8882 --lng -4.7794 --city "Cordoba" --zoom 13
  python3 scripts/pick_city_points.py --center 37.8882,-4.7794 --city "Cordoba" --no-open

The generated HTML works as a small local tool:
  - click the map to add a point;
  - edit/delete points from the sidebar or marker popup;
  - export JSON/CSV for later use in geo-grid benchmark runs.
"""

from __future__ import annotations

import argparse
from functools import partial
import html
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import re
import socket
import sys
import unicodedata
from urllib.parse import quote
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Genera un mapa interactivo local para clicar puntos relevantes de una ciudad "
            "y exportarlos como JSON/CSV."
        )
    )
    parser.add_argument("--center", default="", help="Centro como 'lat,lng'. Ej: 37.8882,-4.7794")
    parser.add_argument("--lat", type=float, default=None, help="Latitud del centro si no usas --center.")
    parser.add_argument("--lng", type=float, default=None, help="Longitud del centro si no usas --center.")
    parser.add_argument("--city", default="city", help="Nombre de ciudad para el artefacto/export (default: city).")
    parser.add_argument("--zoom", type=int, default=13, help="Zoom inicial del mapa (default: 13).")
    parser.add_argument(
        "--output",
        default="",
        help="Ruta HTML de salida (default: artifacts/geo_points/<city>_<timestamp>.html).",
    )
    parser.add_argument(
        "--initial-points",
        default="",
        help="JSON opcional con puntos previos. Acepta lista o payload con clave 'points'.",
    )
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="No abrir el navegador automaticamente ni levantar servidor; solo escribir el HTML.",
    )
    parser.add_argument(
        "--file-url",
        action="store_true",
        help=(
            "Abrir el HTML como file:// en vez de servirlo desde localhost. "
            "No recomendado: algunos proveedores de mapa bloquean file://."
        ),
    )
    return parser.parse_args()


def _slugify(value: str) -> str:
    cleaned = unicodedata.normalize("NFKD", str(value or "").strip().lower())
    cleaned = "".join(ch for ch in cleaned if not unicodedata.combining(ch))
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned).strip("_")
    return cleaned or "city"


def _parse_center(args: argparse.Namespace) -> tuple[float, float]:
    if args.center:
        parts = [part.strip() for part in str(args.center).split(",")]
        if len(parts) != 2:
            raise SystemExit("--center debe tener formato 'lat,lng'. Ej: 37.8882,-4.7794")
        try:
            return float(parts[0]), float(parts[1])
        except ValueError as exc:
            raise SystemExit("--center contiene numeros invalidos.") from exc

    if args.lat is None or args.lng is None:
        raise SystemExit("Debes pasar --center lat,lng o bien --lat y --lng.")
    return float(args.lat), float(args.lng)


def _resolve_output_path(output: str, city: str) -> Path:
    if output:
        path = Path(output).expanduser()
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        return path.resolve()

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return PROJECT_ROOT / "artifacts" / "geo_points" / f"{_slugify(city)}_points_{ts}.html"


def _load_initial_points(path: str) -> list[dict[str, Any]]:
    if not path:
        return []
    raw_path = Path(path).expanduser()
    if not raw_path.is_absolute():
        raw_path = PROJECT_ROOT / raw_path
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    raw_points = payload.get("points", payload) if isinstance(payload, dict) else payload
    if not isinstance(raw_points, list):
        raise SystemExit("--initial-points debe ser una lista JSON o un objeto con clave 'points'.")

    points: list[dict[str, Any]] = []
    for index, point in enumerate(raw_points, start=1):
        if not isinstance(point, dict):
            continue
        lat = point.get("lat", point.get("latitude"))
        lng = point.get("lng", point.get("lon", point.get("longitude")))
        if lat is None or lng is None:
            continue
        try:
            parsed_lat = float(lat)
            parsed_lng = float(lng)
        except (TypeError, ValueError):
            continue
        label = str(point.get("label") or point.get("name") or f"Punto {index}").strip()
        points.append({"order": index, "label": label, "lat": parsed_lat, "lng": parsed_lng})
    return points


def _render_html(*, city: str, center_lat: float, center_lng: float, zoom: int, points: list[dict[str, Any]]) -> str:
    city_json = json.dumps(city, ensure_ascii=False)
    points_json = json.dumps(points, ensure_ascii=False)
    center_json = json.dumps({"lat": center_lat, "lng": center_lng}, ensure_ascii=False)
    title = html.escape(f"Selector de puntos - {city}")
    safe_city = html.escape(city)
    generated_at = datetime.now(timezone.utc).isoformat()

    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <style>
    :root {{
      --bg: #f4efe6;
      --panel: #fffaf0;
      --ink: #251b12;
      --muted: #756755;
      --line: #ded0bd;
      --accent: #a7431f;
      --accent-dark: #7a2e14;
      --ok: #2f6f4e;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: ui-serif, Georgia, "Times New Roman", serif;
    }}
    .app {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) 420px;
      height: 100vh;
      min-height: 620px;
    }}
    #map {{ height: 100%; width: 100%; }}
    aside {{
      display: flex;
      flex-direction: column;
      gap: 14px;
      border-left: 1px solid var(--line);
      background:
        radial-gradient(circle at 15% 5%, rgba(167, 67, 31, 0.12), transparent 30%),
        linear-gradient(180deg, #fffaf0 0%, #f8eddb 100%);
      padding: 18px;
      overflow: auto;
    }}
    h1 {{
      margin: 0;
      font-size: 28px;
      line-height: 1;
      letter-spacing: -0.03em;
    }}
    .subtitle {{
      color: var(--muted);
      font-size: 14px;
      line-height: 1.35;
      margin-top: 6px;
    }}
    .card {{
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(255, 250, 240, 0.86);
      box-shadow: 0 20px 50px rgba(46, 32, 18, 0.09);
      padding: 14px;
    }}
    .row {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }}
    button, .file-label {{
      border: 1px solid rgba(37, 27, 18, 0.14);
      border-radius: 999px;
      background: #fff7e8;
      color: var(--ink);
      cursor: pointer;
      font: 700 13px/1 ui-sans-serif, system-ui, -apple-system, sans-serif;
      padding: 9px 12px;
      transition: transform 0.12s ease, background 0.12s ease, border-color 0.12s ease;
    }}
    button:hover, .file-label:hover {{
      transform: translateY(-1px);
      border-color: rgba(167, 67, 31, 0.45);
      background: #fff0d4;
    }}
    button.primary {{
      background: var(--accent);
      color: #fffaf0;
      border-color: var(--accent);
    }}
    button.primary:hover {{ background: var(--accent-dark); }}
    button.danger {{
      color: #8b1e13;
      background: #fff0ec;
    }}
    input[type="file"] {{ display: none; }}
    .hint {{
      color: var(--muted);
      font: 13px/1.4 ui-sans-serif, system-ui, -apple-system, sans-serif;
    }}
    .count {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 34px;
      height: 28px;
      border-radius: 999px;
      background: #242018;
      color: #fffaf0;
      font: 800 13px/1 ui-sans-serif, system-ui, -apple-system, sans-serif;
    }}
    ol {{
      list-style: none;
      margin: 0;
      padding: 0;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }}
    .point {{
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.52);
      padding: 10px;
    }}
    .point-main {{
      display: grid;
      grid-template-columns: 30px minmax(0, 1fr);
      gap: 8px;
      align-items: start;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 28px;
      height: 28px;
      border-radius: 50%;
      background: var(--accent);
      color: white;
      font: 800 12px/1 ui-sans-serif, system-ui, -apple-system, sans-serif;
    }}
    .point input {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #fffaf0;
      color: var(--ink);
      padding: 8px 9px;
      font: 700 14px/1 ui-sans-serif, system-ui, -apple-system, sans-serif;
    }}
    .coords {{
      margin-top: 5px;
      color: var(--muted);
      font: 12px/1.35 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      word-break: break-all;
    }}
    .point-actions {{
      margin-top: 8px;
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
    }}
    textarea {{
      width: 100%;
      min-height: 180px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #1f1b17;
      color: #fff6e8;
      padding: 12px;
      font: 12px/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      resize: vertical;
    }}
    .status {{
      min-height: 20px;
      color: var(--ok);
      font: 700 13px/1.35 ui-sans-serif, system-ui, -apple-system, sans-serif;
    }}
    .leaflet-popup-content button {{
      margin-right: 6px;
      margin-top: 6px;
    }}
    @media (max-width: 980px) {{
      .app {{ grid-template-columns: 1fr; grid-template-rows: 58vh auto; height: auto; min-height: 100vh; }}
      aside {{ border-left: 0; border-top: 1px solid var(--line); }}
    }}
  </style>
</head>
<body>
  <main class="app">
    <section id="map" aria-label="Mapa para seleccionar puntos"></section>
    <aside>
      <header>
        <h1>{safe_city}</h1>
        <div class="subtitle">
      Clica en el mapa para crear puntos rapido. Luego puedes renombrarlos en la lista y exportarlos como JSON/CSV.
        </div>
      </header>

      <section class="card">
        <div class="row">
          <span class="count" id="point-count">0</span>
          <button class="primary" id="download-json">Descargar JSON</button>
          <button id="download-csv">Descargar CSV</button>
          <button id="copy-json">Copiar JSON</button>
        </div>
        <p class="hint">
          Formato pensado para meterlo luego al geo-grid/deep study: label, lat, lng y orden de seleccion.
        </p>
        <div class="row">
          <label class="file-label" for="import-json">Importar JSON</label>
          <input id="import-json" type="file" accept=".json,application/json" />
          <button id="clear-points" class="danger">Vaciar puntos</button>
          <button id="recenter">Volver al centro</button>
        </div>
        <div class="status" id="status"></div>
      </section>

      <section class="card">
        <ol id="points-list"></ol>
      </section>

      <section class="card">
        <div class="hint" style="margin-bottom: 8px;">Preview JSON</div>
        <textarea id="json-preview" spellcheck="false" readonly></textarea>
      </section>
    </aside>
  </main>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script>
    const CITY = {city_json};
    const CENTER = {center_json};
    const INITIAL_POINTS = {points_json};
    const INITIAL_ZOOM = {int(zoom)};
    const GENERATED_AT = {json.dumps(generated_at)};
    const STORAGE_KEY = `city-points:${{CITY}}:${{CENTER.lat.toFixed(4)}},${{CENTER.lng.toFixed(4)}}`;

    const map = L.map('map', {{ zoomControl: true }}).setView([CENTER.lat, CENTER.lng], INITIAL_ZOOM);
    const baseLayers = {{
      'Carto Light': L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
        maxZoom: 20,
        subdomains: 'abcd',
        attribution: '&copy; OpenStreetMap contributors &copy; CARTO'
      }}),
      'OpenStreetMap': L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
        maxZoom: 19,
        attribution: '&copy; OpenStreetMap contributors'
      }})
    }};
    baseLayers['Carto Light'].addTo(map);
    L.control.layers(baseLayers, null, {{ collapsed: true }}).addTo(map);
    L.control.scale({{ metric: true, imperial: false }}).addTo(map);

    let points = loadPoints();
    const markers = new Map();

    function loadPoints() {{
      try {{
        const stored = window.localStorage.getItem(STORAGE_KEY);
        if (stored) {{
          const parsed = JSON.parse(stored);
          if (Array.isArray(parsed.points)) return normalizePoints(parsed.points);
          if (Array.isArray(parsed)) return normalizePoints(parsed);
        }}
      }} catch (error) {{
        console.warn('No se pudo cargar localStorage', error);
      }}
      return normalizePoints(INITIAL_POINTS);
    }}

    function normalizePoints(rawPoints) {{
      return (rawPoints || [])
        .map((point, index) => ({{
          order: index + 1,
          label: String(point.label || point.name || `Punto ${{index + 1}}`).trim(),
          lat: Number(point.lat ?? point.latitude),
          lng: Number(point.lng ?? point.lon ?? point.longitude)
        }}))
        .filter((point) => Number.isFinite(point.lat) && Number.isFinite(point.lng));
    }}

    function payload() {{
      return {{
        city: CITY,
        center: CENTER,
        generated_at: GENERATED_AT,
        exported_at: new Date().toISOString(),
        points: points.map((point, index) => ({{
          order: index + 1,
          label: point.label || `Punto ${{index + 1}}`,
          lat: Number(point.lat.toFixed(7)),
          lng: Number(point.lng.toFixed(7))
        }}))
      }};
    }}

    function persist() {{
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(payload()));
    }}

    function setStatus(message) {{
      const el = document.getElementById('status');
      el.textContent = message || '';
      if (message) setTimeout(() => {{ if (el.textContent === message) el.textContent = ''; }}, 2500);
    }}

    function renumber() {{
      points = points.map((point, index) => ({{ ...point, order: index + 1 }}));
    }}

    function popupHtml(point, index) {{
      const escapedLabel = escapeHtml(point.label || `Punto ${{index + 1}}`);
      return `
        <strong>${{index + 1}}. ${{escapedLabel}}</strong><br>
        <code>${{point.lat.toFixed(7)}}, ${{point.lng.toFixed(7)}}</code><br>
        <button onclick="window.__cityPointEdit(${{index}})">Editar</button>
        <button onclick="window.__cityPointDelete(${{index}})">Borrar</button>
      `;
    }}

    function syncMarkers() {{
      for (const marker of markers.values()) map.removeLayer(marker);
      markers.clear();
      points.forEach((point, index) => {{
        const marker = L.marker([point.lat, point.lng], {{ draggable: true }}).addTo(map);
        marker.bindPopup(popupHtml(point, index));
        marker.on('dragend', () => {{
          const latlng = marker.getLatLng();
          points[index].lat = latlng.lat;
          points[index].lng = latlng.lng;
          render();
          setStatus('Punto movido.');
        }});
        markers.set(index, marker);
      }});
    }}

    function render() {{
      renumber();
      syncMarkers();
      persist();
      document.getElementById('point-count').textContent = String(points.length);
      document.getElementById('json-preview').value = JSON.stringify(payload(), null, 2);

      const list = document.getElementById('points-list');
      list.innerHTML = '';
      points.forEach((point, index) => {{
        const item = document.createElement('li');
        item.className = 'point';
        item.innerHTML = `
          <div class="point-main">
            <span class="badge">${{index + 1}}</span>
            <div>
              <input value="${{escapeHtml(point.label || `Punto ${{index + 1}}`)}}" aria-label="Nombre del punto ${{index + 1}}" />
              <div class="coords">${{point.lat.toFixed(7)}}, ${{point.lng.toFixed(7)}}</div>
            </div>
          </div>
          <div class="point-actions">
            <button data-action="focus">Ver</button>
            <button data-action="up">Subir</button>
            <button data-action="down">Bajar</button>
            <button data-action="delete" class="danger">Borrar</button>
          </div>
        `;
        const input = item.querySelector('input');
        input.addEventListener('input', () => {{
          points[index].label = input.value.trim();
          persist();
          syncMarkers();
          document.getElementById('json-preview').value = JSON.stringify(payload(), null, 2);
        }});
        item.querySelector('[data-action="focus"]').addEventListener('click', () => focusPoint(index));
        item.querySelector('[data-action="up"]').addEventListener('click', () => movePoint(index, -1));
        item.querySelector('[data-action="down"]').addEventListener('click', () => movePoint(index, 1));
        item.querySelector('[data-action="delete"]').addEventListener('click', () => deletePoint(index));
        list.appendChild(item);
      }});
    }}

    function addPoint(latlng) {{
      const nextIndex = points.length + 1;
      points.push({{ order: nextIndex, label: `Punto ${{nextIndex}}`, lat: latlng.lat, lng: latlng.lng }});
      render();
      setStatus('Punto añadido.');
    }}

    function deletePoint(index) {{
      points.splice(index, 1);
      render();
      setStatus('Punto borrado.');
    }}

    function editPoint(index) {{
      const current = points[index];
      if (!current) return;
      const label = window.prompt('Nombre del punto:', current.label || `Punto ${{index + 1}}`);
      if (label === null) return;
      current.label = label.trim() || `Punto ${{index + 1}}`;
      render();
      setStatus('Punto editado.');
    }}

    function focusPoint(index) {{
      const point = points[index];
      const marker = markers.get(index);
      if (!point || !marker) return;
      map.setView([point.lat, point.lng], Math.max(map.getZoom(), 16));
      marker.openPopup();
    }}

    function movePoint(index, delta) {{
      const target = index + delta;
      if (target < 0 || target >= points.length) return;
      const copy = points[index];
      points[index] = points[target];
      points[target] = copy;
      render();
    }}

    function download(filename, content, type) {{
      const blob = new Blob([content], {{ type }});
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    }}

    function toCsv() {{
      const rows = [['order', 'label', 'lat', 'lng']];
      payload().points.forEach((point) => rows.push([point.order, point.label, point.lat, point.lng]));
      return rows.map((row) => row.map(csvEscape).join(',')).join('\\n') + '\\n';
    }}

    function csvEscape(value) {{
      const text = String(value ?? '');
      if (/[",\\n]/.test(text)) return `"${{text.replaceAll('"', '""')}}"`;
      return text;
    }}

    function escapeHtml(value) {{
      return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
    }}

    window.__cityPointDelete = deletePoint;
    window.__cityPointEdit = editPoint;

    map.on('click', (event) => addPoint(event.latlng));

    document.getElementById('download-json').addEventListener('click', () => {{
      download(`${{slug(CITY)}}_geo_points.json`, JSON.stringify(payload(), null, 2), 'application/json;charset=utf-8');
    }});
    document.getElementById('download-csv').addEventListener('click', () => {{
      download(`${{slug(CITY)}}_geo_points.csv`, toCsv(), 'text/csv;charset=utf-8');
    }});
    document.getElementById('copy-json').addEventListener('click', async () => {{
      await navigator.clipboard.writeText(JSON.stringify(payload(), null, 2));
      setStatus('JSON copiado al portapapeles.');
    }});
    document.getElementById('clear-points').addEventListener('click', () => {{
      if (!points.length || window.confirm('¿Vaciar todos los puntos?')) {{
        points = [];
        render();
        setStatus('Lista vaciada.');
      }}
    }});
    document.getElementById('recenter').addEventListener('click', () => {{
      map.setView([CENTER.lat, CENTER.lng], INITIAL_ZOOM);
    }});
    document.getElementById('import-json').addEventListener('change', async (event) => {{
      const file = event.target.files?.[0];
      if (!file) return;
      const text = await file.text();
      const parsed = JSON.parse(text);
      const imported = Array.isArray(parsed) ? parsed : parsed.points;
      points = normalizePoints(imported || []);
      render();
      setStatus(`Importados ${{points.length}} puntos.`);
      event.target.value = '';
    }});

    function slug(value) {{
      return String(value || 'city')
        .normalize('NFD')
        .replace(/[\\u0300-\\u036f]/g, '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '') || 'city';
    }}

    render();
  </script>
</body>
</html>
"""


class QuietHTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - stdlib signature
        return


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _serve_and_open(output_path: Path) -> None:
    port = _find_free_port()
    handler = partial(QuietHTTPRequestHandler, directory=str(output_path.parent))
    server = ThreadingHTTPServer(("127.0.0.1", port), handler)
    url = f"http://127.0.0.1:{port}/{quote(output_path.name)}"
    print(
        json.dumps({"url": url, "message": "Servidor local activo. Ctrl+C para cerrar."}, ensure_ascii=False, indent=2),
        flush=True,
    )
    opened = webbrowser.open(url)
    if not opened:
        print(f"No se pudo abrir navegador automaticamente. Abre: {url}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor cerrado.")
    finally:
        server.server_close()


def main() -> None:
    args = _parse_args()
    center_lat, center_lng = _parse_center(args)
    output_path = _resolve_output_path(args.output, args.city)
    initial_points = _load_initial_points(args.initial_points)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        _render_html(
            city=args.city,
            center_lat=center_lat,
            center_lng=center_lng,
            zoom=args.zoom,
            points=initial_points,
        ),
        encoding="utf-8",
    )

    print(json.dumps({"html": str(output_path), "points": len(initial_points)}, ensure_ascii=False, indent=2), flush=True)
    if not args.no_open:
        if args.file_url:
            opened = webbrowser.open(output_path.resolve().as_uri())
            if not opened:
                print(f"No se pudo abrir navegador automaticamente. Abre: {output_path}", file=sys.stderr)
        else:
            _serve_and_open(output_path)


if __name__ == "__main__":
    main()
