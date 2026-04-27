"""
PDF Generation Service for RFQ vs Product Master Comparison

This service provides a scalable and reusable way to generate PDF reports
with multiple format types (table, side-by-side, summary).
"""

import io
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from reportlab.lib.colors import (
    HexColor,
    grey,
    white,
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

logger = logging.getLogger(__name__)


class PDFFormatType(str, Enum):
    """PDF Format Types"""

    TABLE = "table"
    SIDE_BY_SIDE = "side_by_side"
    SUMMARY = "summary"


class DataDetailLevel(str, Enum):
    """Data Detail Levels"""

    BASIC = "basic"
    DETAILED = "detailed"
    WITH_STATS = "with_stats"


@dataclass
class PDFConfig:
    """Configuration for PDF generation"""

    page_size: str = "A4"
    margin: float = 0.75  # inches
    title: str = "RFQ vs Product Master Comparison Report"
    show_page_numbers: bool = True
    show_timestamp: bool = True
    company_name: str = "Nippon Paint Marine"

    # Color scheme
    primary_color: str = "#1a365d"  # Dark blue
    secondary_color: str = "#2c5282"  # Medium blue
    accent_color: str = "#3182ce"  # Light blue
    header_bg_color: str = "#2c5282"
    header_text_color: str = "#ffffff"
    alternate_row_color: str = "#ebf8ff"

    # Font sizes
    title_font_size: int = 18
    subtitle_font_size: int = 14
    header_font_size: int = 8
    body_font_size: int = 7

    # Table settings
    table_column_widths: list[float] | None = None

    def to_dict(self) -> dict:
        """Convert config to dictionary"""
        return {
            "page_size": self.page_size,
            "margin": self.margin,
            "title": self.title,
            "show_page_numbers": self.show_page_numbers,
            "show_timestamp": self.show_timestamp,
            "company_name": self.company_name,
        }


@dataclass
class MatchData:
    """Container for match data"""

    rfq_id: str
    rfq_items: list[dict[str, Any]] = field(default_factory=list)
    matches: list[dict[str, Any]] = field(default_factory=list)
    statistics: dict[str, Any] | None = None

    def get_statistics(self) -> dict[str, Any]:
        """Calculate statistics from match data"""
        if self.matches:
            scores = [m.get("score", 0) for m in self.matches]
            return {
                "total_matches": len(self.matches),
                "total_rfqs": len(self.rfq_items),
                "average_score": sum(scores) / len(scores) if scores else 0,
                "max_score": max(scores) if scores else 0,
                "min_score": min(scores) if scores else 0,
                "high_confidence_matches": len([s for s in scores if s >= 85]),
                "medium_confidence_matches": len([s for s in scores if 70 <= s < 85]),
                "low_confidence_matches": len([s for s in scores if s < 70]),
            }
        return {
            "total_matches": 0,
            "total_rfqs": len(self.rfq_items),
            "average_score": 0,
            "max_score": 0,
            "min_score": 0,
            "high_confidence_matches": 0,
            "medium_confidence_matches": 0,
            "low_confidence_matches": 0,
        }


class BasePDFGenerator:
    """Base PDF Generator with common functionality"""

    # Custom style definitions (only for styles that don't exist in default stylesheet)
    STYLES = {
        "rfq_subtitle": {
            "fontName": "Helvetica-Bold",
            "fontSize": 14,
            "alignment": TA_LEFT,
            "textColor": HexColor("#1a365d"),
        },
    }

    def __init__(self, config: PDFConfig):
        self.config = config
        self.story = []
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()

    def _setup_custom_styles(self):
        """Setup custom paragraph styles"""
        for name, style_def in self.STYLES.items():
            # Only add style if it doesn't already exist
            if name not in self.styles:
                self.styles.add(
                    ParagraphStyle(name=name, parent=self.styles["Normal"], **style_def)
                )

    def _get_page_size(self):
        """Get page size from config"""
        return A4 if self.config.page_size == "A4" else letter

    def _create_header(self, title: str, subtitle: str = ""):
        """Create document header"""
        # Use default title style from ReportLab
        self.story.append(Paragraph(title, self.styles["Title"]))
        self.story.append(Spacer(1, 0.1 * inch))

        if subtitle:
            # Create centered subtitle style
            subtitle_style = ParagraphStyle(
                "CenteredSubtitle",
                parent=self.styles["Normal"],
                alignment=TA_CENTER,
                fontSize=10,
                textColor=HexColor("#1a365d"),
                fontName="Helvetica-Bold",
            )
            self.story.append(Paragraph(subtitle, subtitle_style))
            self.story.append(Spacer(1, 0.1 * inch))

        if self.config.show_timestamp:
            # Create centered timestamp style
            timestamp_style = ParagraphStyle(
                "CenteredTimestamp",
                parent=self.styles["Normal"],
                alignment=TA_CENTER,
                fontSize=8,
            )
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.story.append(Paragraph(f"Generated: {timestamp}", timestamp_style))
            self.story.append(Spacer(1, 0.2 * inch))

    def _create_table_style(self, alternating_rows: bool = True) -> TableStyle:
        """Create standard table style"""
        style_commands = [
            ("BACKGROUND", (0, 0), (-1, 0), HexColor(self.config.header_bg_color)),
            ("TEXTCOLOR", (0, 0), (-1, 0), HexColor(self.config.header_text_color)),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), self.config.header_font_size),
            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), self.config.body_font_size),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
            ("TOPPADDING", (0, 0), (-1, 0), 8),
            ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
            ("TOPPADDING", (0, 1), (-1, -1), 4),
            ("GRID", (0, 0), (-1, -1), 0.5, grey),
            (
                "ROWBACKGROUNDS",
                (0, 1),
                (-1, -1),
                [white, HexColor(self.config.alternate_row_color)],
            )
            if alternating_rows
            else None,
        ]

        # Filter out None values
        return TableStyle([cmd for cmd in style_commands if cmd is not None])

    def _create_score_color(self, score: float) -> HexColor:
        """Get color based on match score"""
        if score >= 85:
            return HexColor("#22543d")  # Dark green
        elif score >= 70:
            return HexColor("#744210")  # Brown
        else:
            return HexColor("#742a2a")  # Dark red

    def generate(self, data: MatchData, output_path: str | None = None) -> bytes:
        """Generate PDF - must be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement generate method")

    def _build_pdf(self, story_elements, output_path: str | None = None) -> bytes:
        """Build the final PDF"""
        buffer = io.BytesIO() if output_path is None else None

        if output_path:
            doc = SimpleDocTemplate(
                output_path,
                pagesize=self._get_page_size(),
                leftMargin=self.config.margin * inch,
                rightMargin=self.config.margin * inch,
                topMargin=self.config.margin * inch,
                bottomMargin=self.config.margin * inch,
            )
        else:
            doc = SimpleDocTemplate(
                buffer,
                pagesize=self._get_page_size(),
                leftMargin=self.config.margin * inch,
                rightMargin=self.config.margin * inch,
                topMargin=self.config.margin * inch,
                bottomMargin=self.config.margin * inch,
            )

        doc.build(story_elements)

        if buffer:
            return buffer.getvalue()
        return b""


class TableFormatPDFGenerator(BasePDFGenerator):
    """Table format PDF generator"""

    def generate(self, data: MatchData, output_path: str | None = None) -> bytes:
        """Generate table format PDF"""
        # Create header
        subtitle = f"RFQ ID: {data.rfq_id} | Total Matches: {len(data.matches)}"
        self._create_header(self.config.title, subtitle)

        # Create main matches table
        headers = ["No", "RFQ Item", "Nippon Paint Product", "Match Status"]
        table_data = [headers]

        for idx, match in enumerate(data.matches, 1):
            rfq = match.get("rfq") or {}
            product_master = match.get("product_master") or {}
            match_info = match.get("match_info") or {}
            rfq_text = rfq.get("raw_text", "")[:50]
            product_name = product_master.get("product_name", "")[:40]
            score = match_info.get("score", 0)

            # Determine match status
            match_status = "MATCH" if score > 0 else "NO MATCH"

            row = [str(idx), rfq_text, product_name, match_status]
            table_data.append(row)

        # Create main table with smaller font
        table = Table(
            table_data, colWidths=[0.5 * inch, 2.5 * inch, 2.5 * inch, 1.5 * inch]
        )

        # Create style with smaller font for main matches table
        table_style = TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), HexColor(self.config.header_bg_color)),
                ("TEXTCOLOR", (0, 0), (-1, 0), HexColor(self.config.header_text_color)),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 6),  # Smaller header font
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 1), (-1, -1), 6),  # Smaller body font
                ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
                ("TOPPADDING", (0, 0), (-1, 0), 6),
                ("BOTTOMPADDING", (0, 1), (-1, -1), 4),
                ("TOPPADDING", (0, 1), (-1, -1), 3),
                ("GRID", (0, 0), (-1, -1), 0.5, grey),
                (
                    "ROWBACKGROUNDS",
                    (0, 1),
                    (-1, -1),
                    [white, HexColor(self.config.alternate_row_color)],
                ),
            ]
        )
        table.setStyle(table_style)

        # Color match status cells based on status
        for idx, match in enumerate(data.matches, 1):
            match_info = match.get("match_info") or {}
            score = match_info.get("score", 0)
            # Green for match, red for no match
            color = HexColor("#22543d") if score > 0 else HexColor("#742a2a")
            table.setStyle(
                TableStyle(
                    [
                        ("TEXTCOLOR", (3, idx), (3, idx), color),
                    ]
                )
            )

        self.story.append(table)
        self.story.append(Spacer(1, 0.3 * inch))

        # Create color comparison table
        color_headers = [
            "No",
            "RFQ Item",
            "RFQ Color",
            "Nippon Paint Color",
            "Color Match",
        ]
        color_table_data = [color_headers]

        for idx, match in enumerate(data.matches, 1):
            rfq = match.get("rfq") or {}
            product_master = match.get("product_master") or {}
            match_info = match.get("match_info") or {}
            logger.info(f"_generate_table_format_pdf match_info: {match_info}")
            rfq_text = rfq.get("raw_text", "")[:40]
            # Use normalized_color from OpenAI if available, fallback to color field
            rfq_color = rfq.get("normalized_color") or rfq.get("color", "-") or "-"
            product_color = product_master.get("color", "-") or "-"
            color_match = match_info.get("color_match", False)

            # Color match indicator
            match_indicator = (
                "✓"
                if color_match and rfq_color != "-" and product_color != "-"
                else "✗"
            )

            row = [str(idx), rfq_text, rfq_color, product_color, match_indicator]
            color_table_data.append(row)

        # Create color comparison table with wider RFQ Item column and smaller font
        color_table = Table(
            color_table_data,
            colWidths=[0.5 * inch, 2.8 * inch, 1.2 * inch, 1.2 * inch, 1 * inch],
        )

        # Create style with smaller font for color comparison table
        color_table_style = TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), HexColor(self.config.header_bg_color)),
                ("TEXTCOLOR", (0, 0), (-1, 0), HexColor(self.config.header_text_color)),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 6),  # Smaller header font
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 1), (-1, -1), 6),  # Smaller body font (reduced from 7)
                ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
                ("TOPPADDING", (0, 0), (-1, 0), 6),
                ("BOTTOMPADDING", (0, 1), (-1, -1), 4),
                ("TOPPADDING", (0, 1), (-1, -1), 3),
                ("GRID", (0, 0), (-1, -1), 0.5, grey),
                (
                    "ROWBACKGROUNDS",
                    (0, 1),
                    (-1, -1),
                    [white, HexColor(self.config.alternate_row_color)],
                ),
            ]
        )
        color_table.setStyle(color_table_style)

        # Highlight color matches
        for idx, match in enumerate(data.matches, 1):
            match_info = match.get("match_info") or {}
            color_match = match_info.get("color_match", False)
            if color_match:
                color_table.setStyle(
                    TableStyle(
                        [
                            (
                                "TEXTCOLOR",
                                (4, idx),
                                (4, idx),
                                HexColor("#22543d"),
                            ),  # Green for match
                            ("BACKGROUND", (4, idx), (4, idx), HexColor("#f0fff4")),
                        ]
                    )
                )

        self.story.append(color_table)

        return self._build_pdf(self.story, output_path)


class SideBySideFormatPDFGenerator(BasePDFGenerator):
    """Side-by-side comparison format PDF generator"""

    def generate(self, data: MatchData, output_path: str | None = None) -> bytes:
        """Generate side-by-side comparison PDF"""
        # Create header
        subtitle = f"RFQ ID: {data.rfq_id} | Side-by-Side Comparison"
        self._create_header(self.config.title, subtitle)

        for idx, match in enumerate(data.matches, 1):
            # RFQ Item section
            rfq = match.get("rfq") or {}
            product = match.get("product_master") or {}
            match_info = match.get("match_info") or {}
            score = match_info.get("score", 0)
            match_status = "MATCH" if score > 0 else "NO MATCH"

            # Create comparison header
            self.story.append(
                Paragraph(
                    f"Match #{idx} - Status: {match_status}",
                    self.styles.get("rfq_subtitle", self.styles["Heading2"]),
                )
            )
            self.story.append(Spacer(1, 0.1 * inch))

            # Comparison table
            comparison_data = [
                ["Field", "RFQ Item", "Product Master"],
                [
                    "Text/Name",
                    rfq.get("raw_text", "")[:40],
                    product.get("product_name", "")[:40],
                ],
                ["Qty/UOM", f"{rfq.get('qty', '-')} {rfq.get('uom', '')}", "-"],
                ["Product Code", "-", product.get("pmc", "-")],
                [
                    "Color",
                    rfq.get("normalized_color") or rfq.get("color", "-") or "-",
                    product.get("color", "-") or "-",
                ],
                ["Sheet Type", "-", product.get("sheet_type", "-")],
                ["Match Status", match_status, "-"],
            ]

            comparison_table = Table(
                comparison_data, colWidths=[1.5 * inch, 2.5 * inch, 2.5 * inch]
            )
            comparison_table.setStyle(self._create_table_style())

            self.story.append(comparison_table)
            self.story.append(Spacer(1, 0.2 * inch))

            # Add page break every 5 matches
            if idx % 5 == 0 and idx < len(data.matches):
                self.story.append(PageBreak())

        return self._build_pdf(self.story, output_path)


class SummaryFormatPDFGenerator(BasePDFGenerator):
    """Summary report format PDF generator"""

    def generate(self, data: MatchData, output_path: str | None = None) -> bytes:
        """Generate summary format PDF"""
        # Calculate statistics
        stats = data.get_statistics()

        # Count matches and no matches
        matched_count = sum(
            1 for m in data.matches if (m.get("match_info") or {}).get("score", 0) > 0
        )
        no_match_count = len(data.matches) - matched_count

        # Create header
        subtitle = f"RFQ ID: {data.rfq_id} | Summary Report"
        self._create_header(self.config.title, subtitle)

        # Statistics section
        self.story.append(
            Paragraph(
                "Match Statistics",
                self.styles.get("rfq_subtitle", self.styles["Heading2"]),
            )
        )
        self.story.append(Spacer(1, 0.1 * inch))

        stats_data = [
            ["Metric", "Value"],
            ["Total RFQ Items", str(stats["total_rfqs"])],
            ["Total Matches", str(stats["total_matches"])],
            ["Matched", str(matched_count)],
            ["No Match", str(no_match_count)],
        ]

        stats_table = Table(stats_data, colWidths=[2.5 * inch, 2 * inch])
        stats_table.setStyle(self._create_table_style())
        self.story.append(stats_table)
        self.story.append(Spacer(1, 0.3 * inch))

        # All matches detail table
        if data.matches:
            self.story.append(
                Paragraph(
                    "All Matches Detail",
                    self.styles.get("rfq_subtitle", self.styles["Heading2"]),
                )
            )
            self.story.append(Spacer(1, 0.1 * inch))

            headers = ["No", "RFQ", "Product", "Match Status"]
            table_data = [headers]

            for idx, match in enumerate(data.matches, 1):
                rfq = match.get("rfq") or {}
                product_master = match.get("product_master") or {}
                match_info = match.get("match_info") or {}
                rfq_text = rfq.get("raw_text", "")[:30]
                product_name = product_master.get("product_name", "")[:30]
                score = match_info.get("score", 0)
                match_status = "MATCH" if score > 0 else "NO MATCH"

                table_data.append([str(idx), rfq_text, product_name, match_status])

            detail_table = Table(
                table_data, colWidths=[0.5 * inch, 2.2 * inch, 2.2 * inch, 1.2 * inch]
            )
            detail_table.setStyle(self._create_table_style())

            # Color match status cells
            for idx, match in enumerate(data.matches, 1):
                match_info = match.get("match_info") or {}
                score = match_info.get("score", 0)
                color = HexColor("#22543d") if score > 0 else HexColor("#742a2a")
                detail_table.setStyle(
                    TableStyle(
                        [
                            ("TEXTCOLOR", (3, idx), (3, idx), color),
                        ]
                    )
                )

            self.story.append(detail_table)

        return self._build_pdf(self.story, output_path)


class PDFService:
    """
    Main PDF Service with factory pattern for format selection

    This service provides a single entry point for generating PDFs
    with different formats and configurations.
    """

    # Generator registry for easy extension
    GENERATORS = {
        PDFFormatType.TABLE: TableFormatPDFGenerator,
        PDFFormatType.SIDE_BY_SIDE: SideBySideFormatPDFGenerator,
        PDFFormatType.SUMMARY: SummaryFormatPDFGenerator,
    }

    # Default configurations
    DEFAULT_CONFIGS = {
        PDFFormatType.TABLE: PDFConfig(
            title="RFQ vs Product Master - Table View",
            page_size="A4",
        ),
        PDFFormatType.SIDE_BY_SIDE: PDFConfig(
            title="RFQ vs Product Master - Side by Side Comparison",
            page_size="A4",
        ),
        PDFFormatType.SUMMARY: PDFConfig(
            title="RFQ vs Product Master - Summary Report",
            page_size="A4",
        ),
    }

    @classmethod
    def get_generator(
        cls, format_type: PDFFormatType, config: PDFConfig | None = None
    ) -> BasePDFGenerator:
        """Get PDF generator instance for specified format"""
        generator_class = cls.GENERATORS.get(format_type)

        if not generator_class:
            raise ValueError(f"Unknown format type: {format_type}")

        # Use default config if none provided
        if config is None:
            config = cls.DEFAULT_CONFIGS.get(format_type, PDFConfig())

        return generator_class(config)

    @classmethod
    def generate_pdf(
        cls,
        data: MatchData,
        format_type: PDFFormatType = PDFFormatType.TABLE,
        config: PDFConfig | None = None,
        output_path: str | None = None,
    ) -> bytes:
        """
        Generate PDF with specified format

        Args:
            data: MatchData containing RFQ items and matches
            format_type: Type of PDF format to generate
            config: Optional PDF configuration
            output_path: Optional file path to save PDF

        Returns:
            PDF bytes
        """
        generator = cls.get_generator(format_type, config)
        return generator.generate(data, output_path)

    @classmethod
    def generate_pdf_from_dict(
        cls,
        rfq_id: str,
        rfq_items: list[dict[str, Any]],
        matches: list[dict[str, Any]],
        format_type: PDFFormatType = PDFFormatType.TABLE,
        config: PDFConfig | None = None,
        output_path: str | None = None,
    ) -> bytes:
        """
        Generate PDF from dictionary data

        Args:
            rfq_id: RFQ identifier
            rfq_items: List of RFQ item dictionaries
            matches: List of match result dictionaries
            format_type: Type of PDF format to generate
            config: Optional PDF configuration
            output_path: Optional file path to save PDF

        Returns:
            PDF bytes
        """
        data = MatchData(
            rfq_id=rfq_id,
            rfq_items=rfq_items,
            matches=matches,
        )
        return cls.generate_pdf(data, format_type, config, output_path)

    @classmethod
    def list_formats(cls) -> list[str]:
        """List available PDF formats"""
        return [fmt.value for fmt in PDFFormatType]

    @classmethod
    def register_generator(
        cls,
        format_type: str,
        generator_class: type,
        default_config: PDFConfig | None = None,
    ):
        """
        Register a new PDF generator format

        This allows for easy extension of the service with new formats.

        Args:
            format_type: String identifier for the format
            generator_class: Generator class (must inherit from BasePDFGenerator)
            default_config: Optional default configuration for this format
        """
        enum_type = PDFFormatType(format_type)
        cls.GENERATORS[enum_type] = generator_class
        if default_config:
            cls.DEFAULT_CONFIGS[enum_type] = default_config


# Singleton instance for easy import
pdf_service = PDFService()
