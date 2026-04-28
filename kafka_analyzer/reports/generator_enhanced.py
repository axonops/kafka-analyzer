"""
Markdown / JSON report generator for Kafka analysis results.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List

import structlog
from jinja2 import Environment, FileSystemLoader

from ..models import ClusterState
from ..models.recommendations import Severity

try:
    from .pdf_generator import PDFGenerator
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False


logger = structlog.get_logger()


SECTION_ORDER = [
    "infrastructure",
    "configuration",
    "operations",
    "topics",
    "security",
    "connect",
    "schema_registry",
]
SECTION_TITLES = {
    "infrastructure": "Infrastructure",
    "configuration": "Broker Configuration",
    "operations": "Operations",
    "topics": "Topics & Consumers",
    "security": "Security",
    "connect": "Kafka Connect",
    "schema_registry": "Schema Registry",
}


class EnhancedReportGenerator:
    """Generates a Kafka analysis report in Markdown (+ JSON, + optional PDF)."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        template_dir = Path(__file__).parent / "templates"
        self.env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=False,
        )
        self.env.filters["severity_icon"] = self._severity_icon
        self.env.filters["severity_text"] = self._severity_text
        self.env.filters["format_number"] = self._format_number

    def generate(self, report_data: Dict[str, Any], generate_pdf: bool = False) -> Path:
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        cluster_name = report_data["cluster_info"]["cluster_name"]

        md_path = self.output_dir / f"kafka_analysis_{cluster_name}_{timestamp}.md"
        self._generate_markdown(report_data, md_path)

        json_path = self.output_dir / f"kafka_analysis_{cluster_name}_{timestamp}.json"
        self._generate_json(report_data, json_path)

        if generate_pdf:
            if not PDF_AVAILABLE:
                logger.warning(
                    "PDF generation requested but dependencies not installed. "
                    "Install with: pip install weasyprint markdown beautifulsoup4"
                )
            else:
                try:
                    pdf_generator = PDFGenerator()
                    pdf_path = pdf_generator.generate_pdf(md_path)
                    logger.info("PDF report generated", path=str(pdf_path))
                except Exception as e:
                    logger.error("Failed to generate PDF", error=str(e))

        return md_path

    # --------------------------------------------------------------- markdown

    def _generate_markdown(self, report_data: Dict[str, Any], output_path: Path) -> None:
        cluster_state: ClusterState = report_data["cluster_state"]
        analysis_results: Dict[str, Any] = report_data["analysis_results"]
        cluster_info = report_data["cluster_info"]

        all_recs = self._all_recommendations(analysis_results)
        severity_counts = self._severity_counts(all_recs)

        # Render each enabled section.
        sections_md: List[str] = []
        for section_name in SECTION_ORDER:
            section_data = analysis_results.get(section_name)
            if section_data is None:
                continue
            sections_md.append(self._render_section(section_name, section_data))

        template = self.env.get_template("report.md")
        content = template.render(
            cluster_info=cluster_info,
            cluster_state=cluster_state,
            severity_counts=severity_counts,
            total_recommendations=len(all_recs),
            top_recommendations=self._top_recommendations(all_recs, limit=10),
            sections_md="\n\n".join(sections_md),
            generated_at=datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
        )

        output_path.write_text(content)
        logger.info("Markdown report generated", path=str(output_path))

    def _render_section(self, name: str, data: Dict[str, Any]) -> str:
        title = SECTION_TITLES.get(name, name.title())

        if data.get("error"):
            return (
                f"## {title}\n\n"
                f"> Analysis failed: `{data['error']}`\n"
            )

        recs = data.get("recommendations") or []
        summary = data.get("summary") or {}
        details = data.get("details") or {}

        lines = [f"## {title}", ""]

        if summary:
            lines.append("### Summary")
            for key, value in summary.items():
                lines.append(f"- **{key.replace('_', ' ').title()}**: {value}")
            lines.append("")

        if recs:
            lines.append("### Findings")
            for rec in recs:
                rec_dict = rec.dict() if hasattr(rec, "dict") else dict(rec)
                lines.append(self._render_recommendation(rec_dict))
        else:
            lines.append("_No issues found in this section._")

        if details:
            lines.append("\n<details><summary>Detailed metrics</summary>\n")
            lines.append("```json")
            try:
                lines.append(json.dumps(details, indent=2, default=self._json_default))
            except TypeError:
                lines.append(str(details))
            lines.append("```")
            lines.append("\n</details>")

        return "\n".join(lines)

    def _render_recommendation(self, rec: Dict[str, Any]) -> str:
        severity = (rec.get("severity") or "info").upper()
        title = rec.get("title", "Recommendation")
        out = [f"\n#### {self._severity_icon(severity)} {title}", ""]
        out.append(f"**Severity:** {severity}  ")
        if rec.get("current_value"):
            out.append(f"**Current value:** {rec['current_value']}  ")
        out.append("")
        if rec.get("description"):
            out.append(rec["description"])
            out.append("")
        if rec.get("impact"):
            out.append(f"**Impact:** {rec['impact']}")
            out.append("")
        if rec.get("recommendation"):
            out.append(f"**Recommendation:** {rec['recommendation']}")
            out.append("")
        ctx = rec.get("context") or {}
        # Surface short lists (e.g. topic names) inline.
        for key, value in ctx.items():
            if isinstance(value, list) and value and len(value) <= 25:
                out.append(f"_{key}:_ `{', '.join(map(str, value))}`")
                out.append("")
        return "\n".join(out)

    # ---------------------------------------------------------------- helpers

    def _all_recommendations(self, analysis_results: Dict[str, Any]) -> List[Any]:
        out: List[Any] = []
        for section in analysis_results.values():
            for rec in section.get("recommendations", []) or []:
                out.append(rec)
        return out

    def _top_recommendations(self, recs: List[Any], limit: int = 10) -> List[Dict[str, Any]]:
        order = {"critical": 0, "warning": 1, "info": 2}
        sorted_recs = sorted(
            (r.dict() if hasattr(r, "dict") else dict(r) for r in recs),
            key=lambda r: order.get((r.get("severity") or "info").lower(), 3),
        )
        return sorted_recs[:limit]

    @staticmethod
    def _severity_counts(recs: List[Any]) -> Dict[str, int]:
        counts = {"critical": 0, "warning": 0, "info": 0}
        for rec in recs:
            sev = getattr(rec, "severity", None)
            if sev is None and isinstance(rec, dict):
                sev = rec.get("severity")
            sev_str = (
                sev.value if isinstance(sev, Severity) else (str(sev) if sev else "info")
            ).lower()
            counts[sev_str] = counts.get(sev_str, 0) + 1
        return counts

    @staticmethod
    def _severity_icon(severity: str) -> str:
        s = (severity or "info").lower()
        return {"critical": "[CRITICAL]", "warning": "[WARNING]", "info": "[INFO]"}.get(
            s, "[INFO]"
        )

    @staticmethod
    def _severity_text(severity: str) -> str:
        return (severity or "info").upper()

    @staticmethod
    def _format_number(value: Any) -> str:
        try:
            return f"{int(value):,}"
        except (TypeError, ValueError):
            try:
                return f"{float(value):,.2f}"
            except (TypeError, ValueError):
                return str(value)

    @staticmethod
    def _json_default(value: Any) -> Any:
        if isinstance(value, (set, frozenset)):
            return list(value)
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    # ------------------------------------------------------------------ json

    def _generate_json(self, report_data: Dict[str, Any], output_path: Path) -> None:
        cluster_state: ClusterState = report_data["cluster_state"]
        payload = {
            "cluster_info": report_data["cluster_info"],
            "summary": {
                "broker_count": cluster_state.get_total_brokers(),
                "topic_count": len(cluster_state.topics),
                "consumer_group_count": len(cluster_state.consumer_groups),
                "under_replicated_partitions": cluster_state.under_replicated_partition_count(),
                "offline_partitions": cluster_state.offline_partition_count(),
            },
            "analysis_results": {
                name: {
                    "summary": section.get("summary"),
                    "details": _json_safe(section.get("details")),
                    "recommendations": [
                        r.dict() if hasattr(r, "dict") else r
                        for r in section.get("recommendations", [])
                    ],
                    "error": section.get("error"),
                }
                for name, section in report_data["analysis_results"].items()
            },
        }
        output_path.write_text(json.dumps(payload, indent=2, default=self._json_default))


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
