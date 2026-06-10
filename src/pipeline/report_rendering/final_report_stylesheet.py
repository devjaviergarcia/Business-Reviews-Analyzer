from __future__ import annotations

from typing import Any


def build_final_report_stylesheet(*, renderer: Any, font_face_css: str) -> str:
    return f"""
      {font_face_css}
      :root {{
        --bg: #F4F2EC;
        --text: #161616;
        --muted: #64748B;
        --panel: #FFFFFF;
        --line: rgba(0, 0, 0, 0.08);
        --accent-1: {renderer._PALETTE[0]};
        --accent-2: {renderer._PALETTE[1]};
        --accent-3: {renderer._PALETTE[2]};
        --accent-4: {renderer._PALETTE[3]};
        --accent-5: {renderer._PALETTE[4]};
        --accent-6: {renderer._PALETTE[5]};
        --good: #12B08A;
        --warn: #D4950A;
        --bad: #C23B18;
        --font-display: "Syne", sans-serif;
        --font-body: "Plus Jakarta Sans", sans-serif;
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        color: var(--text);
        background: var(--bg);
        font-family: var(--font-body);
        line-height: 1.45;
      }}
      .wrap {{
        max-width: 1040px;
        margin: 0 auto;
        padding: 24px;
      }}
      .header {{
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 18px 20px;
        margin-bottom: 14px;
      }}
      .header h1 {{
        margin: 0 0 6px 0;
        font-size: 26px;
        font-family: var(--font-display);
        font-weight: 700;
      }}
      .meta {{
        color: var(--muted);
        font-size: 13px;
      }}
      .intro, .section {{
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 14px 16px;
        margin-bottom: 12px;
      }}
      .context-banner {{
        border-left: 3px solid var(--accent-1);
        border-top-left-radius: 0;
        border-bottom-left-radius: 0;
      }}
      .context-row {{
        display: flex;
        flex-wrap: wrap;
        gap: 18px;
        align-items: center;
        margin-bottom: 4px;
      }}
      .context-item {{
        font-size: 13px;
        color: var(--muted);
      }}
      .icon-slot {{
        display: inline-flex;
        width: 14px;
        height: 14px;
        border-radius: 4px;
        border: 1px solid rgba(10, 117, 103, 0.35);
        background: rgba(212, 240, 232, 0.45);
        margin-right: 6px;
        vertical-align: -2px;
      }}
      .context-item strong {{
        color: var(--text);
      }}
      .section--diagnostico {{ border-left: 3px solid var(--accent-1); border-top-left-radius: 0; border-bottom-left-radius: 0; }}
      .section--puntuacion {{ border-left: 3px solid var(--accent-2); border-top-left-radius: 0; border-bottom-left-radius: 0; }}
      .section--cliente {{ border-left: 3px solid var(--accent-3); border-top-left-radius: 0; border-bottom-left-radius: 0; }}
      .section--accion {{ border-left: 3px solid var(--accent-5); border-top-left-radius: 0; border-bottom-left-radius: 0; }}
      .section--anexo {{ border-left: 3px solid var(--accent-6); border-top-left-radius: 0; border-bottom-left-radius: 0; }}
      h2 {{
        margin: 0 0 10px 0;
        font-size: 18px;
        font-family: var(--font-display);
        font-weight: 700;
      }}
      h3 {{
        margin: 10px 0 6px 0;
        font-size: 15px;
        font-family: var(--font-display);
        font-weight: 700;
      }}
      p {{
        margin: 6px 0;
      }}
      ul {{
        margin: 6px 0 6px 18px;
        padding: 0;
      }}
      li {{
        margin: 3px 0;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
        margin-top: 8px;
      }}
      th, td {{
        border: 1px solid var(--line);
        padding: 7px 8px;
        font-size: 12px;
        vertical-align: top;
      }}
      th {{
        background: var(--accent-3);
        text-align: left;
      }}
      .muted {{ color: var(--muted); }}
      .pill-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 8px;
        margin-top: 8px;
      }}
      .pill {{
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 8px 10px;
        font-size: 12px;
      }}
      .score-hero {{
        display: grid;
        grid-template-columns: 240px 1fr;
        gap: 12px;
      }}
      .score-card {{
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 14px;
        background: var(--panel);
        border-left: 3px solid var(--accent-2);
        border-top-left-radius: 0;
        border-bottom-left-radius: 0;
      }}
      .score-value {{
        font-size: 48px;
        line-height: 1;
        font-weight: 800;
      }}
      .score-label {{
        margin-top: 6px;
        font-size: 13px;
        font-weight: 600;
      }}
      .score-bar-wrap {{
        margin-top: 12px;
      }}
      .score-bar-track {{
        position: relative;
        height: 8px;
        border-radius: 4px;
        border: 1px solid var(--line);
      }}
      .score-bar-zones {{
        display: flex;
        height: 100%;
        border-radius: 4px;
        overflow: hidden;
      }}
      .zone {{
        flex: 1;
      }}
      .zone-red {{ background: var(--bad); flex: 0.55; }}
      .zone-orange {{ background: var(--warn); flex: 0.15; }}
      .zone-yellow {{ background: var(--accent-3); flex: 0.15; }}
      .zone-green {{ background: var(--good); flex: 0.15; }}
      .score-bar-marker {{
        position: absolute;
        top: -4px;
        width: 16px;
        height: 16px;
        border-radius: 50%;
        border: 2px solid #fff;
        transform: translateX(-50%);
      }}
      .score-bar-labels {{
        display: flex;
        justify-content: space-between;
        font-size: 10px;
        color: var(--muted);
        margin-top: 4px;
      }}
      .cluster-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 8px;
        margin-top: 8px;
      }}
      .cluster-card {{
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 10px;
        background: var(--panel);
      }}
      .timeline {{
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 8px;
        margin-top: 8px;
      }}
      .timeline-col {{
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 10px;
      }}
      .timeline-col h4 {{
        margin: 0 0 6px 0;
        font-size: 13px;
      }}
      .timeline-col:nth-child(1) .action-card {{ border-left: 3px solid var(--warn); }}
      .timeline-col:nth-child(2) .action-card {{ border-left: 3px solid var(--accent-2); }}
      .timeline-col:nth-child(3) .action-card {{ border-left: 3px solid var(--accent-6); }}
      .timeline-col:nth-child(1) h4 {{ color: var(--warn); }}
      .timeline-col:nth-child(2) h4 {{ color: var(--accent-2); }}
      .timeline-col:nth-child(3) h4 {{ color: var(--accent-6); }}
      .scatter {{
        margin-top: 10px;
        border: 1px solid var(--line);
        border-radius: 12px;
        background: var(--panel);
        padding: 6px;
      }}
      .scatter-note {{
        font-size: 11px;
        color: var(--muted);
        margin-top: 4px;
      }}
      .bar-chart-wrap {{
        margin-top: 8px;
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 8px;
        background: var(--panel);
      }}
      .action-list {{
        list-style: none;
        margin: 0;
        padding: 0;
        display: grid;
        gap: 8px;
      }}
      .action-card {{
        border: 1px solid var(--line);
        border-radius: 10px;
        padding: 8px 10px;
        background: #fff;
      }}
      .action-card .title {{
        font-weight: 600;
        margin-bottom: 4px;
      }}
      .action-card-header {{
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 8px;
        margin-bottom: 4px;
      }}
      .tipo-badge {{
        display: inline-block;
        font-size: 10px;
        font-weight: 600;
        border-radius: 999px;
        border: 1px solid;
        padding: 2px 8px;
        white-space: nowrap;
        flex-shrink: 0;
      }}
      .urgent-block {{
        background: rgba(194, 59, 24, 0.08);
        border: 1px solid rgba(194, 59, 24, 0.26);
        border-left: 4px solid var(--warn);
        border-radius: 0 12px 12px 0;
        padding: 12px 14px;
        margin: 12px 0;
      }}
      .urgent-title {{
        color: var(--bad);
        font-size: 14px;
        margin: 0 0 8px 0;
      }}
      .fw-grid {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 16px;
        margin-top: 10px;
      }}
      .fw-col-title {{
        font-size: 14px;
        font-weight: 700;
        margin: 0 0 10px 0;
        padding-bottom: 6px;
        border-bottom: 2px solid currentColor;
      }}
      .fw-col-strong {{ color: var(--good); }}
      .fw-col-weak {{ color: var(--warn); }}
      .fw-card {{
        display: flex;
        gap: 10px;
        border-radius: 10px;
        padding: 10px 12px;
        margin-bottom: 8px;
      }}
      .fw-strong {{
        background: rgba(18, 176, 138, 0.10);
        border: 1px solid rgba(18, 176, 138, 0.28);
      }}
      .fw-weak {{
        background: rgba(212, 149, 10, 0.12);
        border: 1px solid rgba(212, 149, 10, 0.28);
      }}
      .fw-icon {{
        display: inline-flex;
        width: 16px;
        height: 16px;
        flex-shrink: 0;
        margin-top: 2px;
      }}
      .fw-strong .fw-icon {{ color: var(--good); }}
      .fw-weak .fw-icon {{ color: var(--warn); }}
      .fw-title {{
        font-weight: 600;
        font-size: 13px;
        margin-bottom: 3px;
      }}
      .fw-desc {{
        font-size: 12px;
        color: var(--muted);
        margin-bottom: 4px;
      }}
      .fw-action {{
        font-size: 12px;
      }}
      .fw-tipo-badge {{
        display: inline-block;
        font-size: 11px;
        background: rgba(212, 149, 10, 0.14);
        color: #8a6209;
        border: 1px solid rgba(212, 149, 10, 0.30);
        border-radius: 999px;
        padding: 2px 8px;
      }}
      .annex-details {{
        cursor: pointer;
      }}
      .annex-summary {{
        font-weight: 600;
        font-size: 14px;
        color: var(--muted);
        list-style: none;
      }}
      .annex-summary::-webkit-details-marker {{ display: none; }}
      .annex-hint {{
        font-weight: 400;
        font-size: 12px;
      }}
      .annex-body {{
        border-top: 1px solid var(--line);
        padding-top: 12px;
        margin-top: 10px;
      }}
      .meta-line {{
        color: var(--muted);
        font-size: 12px;
      }}
      .voice-list {{
        list-style: none;
        margin: 0;
        padding: 0;
        display: grid;
        gap: 8px;
      }}
      .voice-card {{
        border: 1px solid var(--line);
        border-radius: 10px;
        padding: 8px 10px;
        background: #fff;
      }}
      .voice-meta {{
        color: var(--muted);
        font-size: 12px;
        margin-bottom: 4px;
      }}
      .metric-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
        gap: 8px;
        margin-top: 8px;
      }}
      .metric-card {{
        border: 1px solid var(--line);
        border-radius: 10px;
        padding: 8px 10px;
        background: #fff;
      }}
      .metric-title {{
        font-weight: 600;
        margin-bottom: 4px;
      }}
      .metric-value {{
        font-size: 20px;
        font-weight: 700;
        line-height: 1.1;
      }}
      .metric-explain {{
        color: var(--muted);
        font-size: 12px;
      }}
      .badge {{
        display: inline-block;
        border-radius: 999px;
        font-size: 11px;
        padding: 2px 8px;
        border: 1px solid transparent;
      }}
      .badge.good {{ background: #e7fbef; color: var(--good); border-color: #c5f1d7; }}
      .badge.warn {{ background: rgba(212, 149, 10, 0.12); color: #8a6209; border-color: rgba(212, 149, 10, 0.30); }}
      .badge.bad {{ background: #ffe8e8; color: #ab2329; border-color: #ffc9cb; }}
      .footer {{
        color: var(--muted);
        text-align: center;
        margin-top: 16px;
        font-size: 12px;
      }}
      @media (max-width: 820px) {{
        .score-hero {{ grid-template-columns: 1fr; }}
        .timeline {{ grid-template-columns: 1fr; }}
        .fw-grid {{ grid-template-columns: 1fr; }}
      }}
    """
