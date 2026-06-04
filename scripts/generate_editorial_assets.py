#!/usr/bin/env python3
"""Generate editorial image assets for LinkedIn posts."""

from __future__ import annotations

from pathlib import Path
from textwrap import wrap

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "docs" / "editorial" / "assets"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

WIDTH = 1200
HEIGHT = 1200
PADDING = 72

BG = "#F4F1EA"
CARD = "#FFFFFF"
INK = "#102533"
MUTED = "#4B5563"
ACCENT = "#1E5B74"


def _load_font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for path in candidates:
        p = Path(path)
        if p.exists():
            return ImageFont.truetype(str(p), size=size)
    return ImageFont.load_default()


def _draw_wrapped(draw: ImageDraw.ImageDraw, text: str, *, x: int, y: int, font, fill: str, width_chars: int, line_gap: int = 8) -> int:
    lines = wrap(text, width=width_chars)
    for line in lines:
        draw.text((x, y), line, font=font, fill=fill)
        y += int(font.size * 1.2) + line_gap if hasattr(font, "size") else 28 + line_gap
    return y


def make_card(filename: str, title: str, value: str, insight: str, cta: str) -> None:
    image = Image.new("RGB", (WIDTH, HEIGHT), BG)
    draw = ImageDraw.Draw(image)

    draw.rounded_rectangle((40, 40, WIDTH - 40, HEIGHT - 40), radius=32, fill=CARD, outline="#D8DDE3", width=2)
    draw.rounded_rectangle((PADDING, PADDING, WIDTH - PADDING, 190), radius=18, fill="#E9F0F4", outline="#D2DEE6")

    eyebrow_font = _load_font(28, bold=True)
    title_font = _load_font(56, bold=True)
    value_font = _load_font(110, bold=True)
    body_font = _load_font(36, bold=False)
    cta_font = _load_font(30, bold=True)

    draw.text((PADDING + 24, PADDING + 22), "ESTUDIO LOCAL REPIQ", font=eyebrow_font, fill=ACCENT)
    y = 230
    y = _draw_wrapped(draw, title, x=PADDING, y=y, font=title_font, fill=INK, width_chars=25, line_gap=6)

    draw.rounded_rectangle((PADDING, y + 22, WIDTH - PADDING, y + 240), radius=24, fill="#F7FAFC", outline="#DCE7EF")
    draw.text((PADDING + 26, y + 54), value, font=value_font, fill=ACCENT)

    y = y + 280
    y = _draw_wrapped(draw, insight, x=PADDING, y=y, font=body_font, fill=MUTED, width_chars=44, line_gap=10)

    draw.rounded_rectangle((PADDING, HEIGHT - 190, WIDTH - PADDING, HEIGHT - 90), radius=50, fill=ACCENT)
    draw.text((PADDING + 28, HEIGHT - 160), cta, font=cta_font, fill="#FFFFFF")

    image.save(OUTPUT_DIR / filename, format="PNG")


def main() -> None:
    cards = [
        (
            "linkedin_post_01.png",
            "La mitad de la muestra no muestra web en ficha",
            "50%",
            "Si no hay web o CTA claro, se pierde conversion aunque el rating sea bueno.",
            "Ver estudio local completo - Cordoba",
        ),
        (
            "linkedin_post_02.png",
            "Mucho volumen de resenas no siempre mejora conversion",
            "1.146",
            "La prueba social ayuda, pero sin ficha completa el cliente no llega al siguiente paso.",
            "Comparar tu ficha con competidores",
        ),
        (
            "linkedin_post_03.png",
            "Oportunidad real: ordenar ficha + respuesta de resenas",
            "Top 3",
            "Pequenos cambios operativos en la ficha impactan mas que perseguir decimas de rating.",
            "Pedir informe individual gratis",
        ),
    ]

    for filename, title, value, insight, cta in cards:
        make_card(filename, title, value, insight, cta)

    print(f"Generated {len(cards)} image assets in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
