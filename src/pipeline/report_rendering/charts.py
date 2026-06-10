from __future__ import annotations

from typing import Any

from .customer_segment_bar_chart_renderer import (
    render_customer_segment_bar_chart,
    render_customer_weight_summary_chart,
)
from .customer_segment_scatter_renderer import (
    render_customer_segment_scatter,
    render_generic_scatter_chart,
)
from .helpers import ReportRenderingHelpersMixin


class ReportRenderingChartsMixin(ReportRenderingHelpersMixin):
    def _render_bar_chart_vista_c(self, bar_chart_data: dict[str, Any]) -> str:
        return render_customer_segment_bar_chart(self, bar_chart_data)

    def _render_customer_bar_chart(self, scatter_payload: dict[str, Any]) -> str:
        return render_customer_weight_summary_chart(self, scatter_payload)

    def _render_scatter_vista_d(self, scatter_data: dict[str, Any]) -> str:
        return render_customer_segment_scatter(self, scatter_data)

    def _maybe_render_scatter_svg(self, payload: dict[str, Any]) -> str | None:
        return render_generic_scatter_chart(self, payload)
