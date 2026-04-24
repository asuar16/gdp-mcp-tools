"""Google Doc tools for GDP MCP server.

Creates branded Google Docs with Grubhub/Wonder colors and proper HTML tables.
Wraps the Google Workspace MCP's import_to_google_doc with built-in CSS template.
"""

import json
import logging
import subprocess

logger = logging.getLogger(__name__)

# Grubhub / Wonder brand colors
GH_ORANGE = "#FF8000"
WONDER_PURPLE = "#7B1FA2"
GREEN_HIGHLIGHT = "#E8F5E9"
LIGHT_RED_ALERT = "#FFEBEE"
LIGHT_ORANGE_ROW = "#FFF8F0"
ORANGE_DIVIDER = "#FFD699"
SUCCESS_GREEN = "#4CAF50"
WARN_YELLOW = "#FFC107"

CSS_TEMPLATE = f"""
<style>
  body {{ font-family: Arial, sans-serif; font-size: 11pt; color: #333; line-height: 1.6; }}
  h1 {{ font-size: 20pt; color: {GH_ORANGE}; border-bottom: 3px solid {GH_ORANGE}; padding-bottom: 6px; }}
  h2 {{ font-size: 16pt; color: {GH_ORANGE}; margin-top: 24px; }}
  h3 {{ font-size: 13pt; color: #333; margin-top: 16px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 12px 0; }}
  th {{ background-color: {GH_ORANGE}; color: white; padding: 8px 12px; text-align: left; font-size: 10pt; }}
  td {{ border: 1px solid #ddd; padding: 6px 12px; font-size: 10pt; }}
  tr:nth-child(even) {{ background-color: {LIGHT_ORANGE_ROW}; }}
  .highlight {{ background-color: {GREEN_HIGHLIGHT}; font-weight: bold; }}
  .alert {{ background-color: {LIGHT_RED_ALERT}; font-weight: bold; }}
  .code {{ font-family: 'Courier New', monospace; background-color: #f5f5f5; padding: 2px 6px; font-size: 10pt; }}
  .codeblock {{ font-family: 'Courier New', monospace; background-color: #f5f5f5; padding: 12px; margin: 8px 0; font-size: 9pt; white-space: pre-wrap; border: 1px solid #ddd; border-radius: 4px; }}
  .link {{ color: {GH_ORANGE}; }}
  .section-divider {{ border-top: 2px solid {ORANGE_DIVIDER}; margin: 24px 0; }}
  .callout {{ background-color: #FFF3E0; border-left: 4px solid {GH_ORANGE}; padding: 12px 16px; margin: 12px 0; }}
  .callout-warn {{ background-color: #FFF8E1; border-left: 4px solid {WARN_YELLOW}; padding: 12px 16px; margin: 12px 0; }}
  .callout-success {{ background-color: {GREEN_HIGHLIGHT}; border-left: 4px solid {SUCCESS_GREEN}; padding: 12px 16px; margin: 12px 0; }}
  .callout-wonder {{ background-color: #F3E5F5; border-left: 4px solid {WONDER_PURPLE}; padding: 12px 16px; margin: 12px 0; }}
  .metric-big {{ font-size: 18pt; font-weight: bold; color: {GH_ORANGE}; }}
  .wonder-text {{ color: {WONDER_PURPLE}; font-weight: bold; }}
  .gh-text {{ color: {GH_ORANGE}; font-weight: bold; }}
</style>
"""


def _md_table_to_html(md_text):
    """Convert markdown tables in text to HTML tables.

    Handles:
    - Standard markdown tables with | delimiters
    - Bold (**text**) -> <b>text</b>
    - Inline code (`text`) -> <span class="code">text</span>
    - Links [text](url) -> <a class="link" href="url">text</a>
    - Headers (# ## ###) -> <h1> <h2> <h3>
    - Horizontal rules (---) -> <div class="section-divider"></div>
    - Paragraphs
    """
    lines = md_text.strip().split("\n")
    html_parts = []
    in_table = False
    in_codeblock = False
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        # Code blocks
        if line.startswith("```"):
            if in_codeblock:
                html_parts.append("</div>")
                in_codeblock = False
            else:
                html_parts.append('<div class="codeblock">')
                in_codeblock = True
            i += 1
            continue

        if in_codeblock:
            html_parts.append(line.replace("<", "&lt;").replace(">", "&gt;"))
            html_parts.append("<br>")
            i += 1
            continue

        # Horizontal rule / section divider
        if line == "---" or line == "***":
            if in_table:
                html_parts.append("</table>")
                in_table = False
            html_parts.append('<div class="section-divider"></div>')
            i += 1
            continue

        # Headers
        if line.startswith("### "):
            if in_table:
                html_parts.append("</table>")
                in_table = False
            html_parts.append(f"<h3>{_inline_format(line[4:])}</h3>")
            i += 1
            continue
        if line.startswith("## "):
            if in_table:
                html_parts.append("</table>")
                in_table = False
            html_parts.append(f"<h2>{_inline_format(line[3:])}</h2>")
            i += 1
            continue
        if line.startswith("# "):
            if in_table:
                html_parts.append("</table>")
                in_table = False
            html_parts.append(f"<h1>{_inline_format(line[2:])}</h1>")
            i += 1
            continue

        # Table rows
        if "|" in line and line.startswith("|"):
            cells = [c.strip() for c in line.split("|")[1:-1]]

            # Skip separator rows (|---|---|)
            if all(c.replace("-", "").replace(":", "") == "" for c in cells):
                i += 1
                continue

            if not in_table:
                html_parts.append("<table>")
                in_table = True
                # First row is header
                html_parts.append("<tr>")
                for cell in cells:
                    html_parts.append(f"<th>{_inline_format(cell)}</th>")
                html_parts.append("</tr>")
            else:
                # Check for highlight/alert classes
                row_class = ""
                if any(c.startswith("**") and c.endswith("**") for c in cells):
                    row_class = ""  # let individual cells handle bold
                cls_attr = ' class="' + row_class + '"' if row_class else ''
                html_parts.append(f"<tr{cls_attr}>")
                for cell in cells:
                    html_parts.append(f"<td>{_inline_format(cell)}</td>")
                html_parts.append("</tr>")
            i += 1
            continue

        # End of table
        if in_table and not ("|" in line and line.startswith("|")):
            html_parts.append("</table>")
            in_table = False

        # Empty line
        if not line:
            i += 1
            continue

        # Bullet points
        if line.startswith("- "):
            html_parts.append(f"<p>&bull; {_inline_format(line[2:])}</p>")
            i += 1
            continue

        # Regular paragraph
        html_parts.append(f"<p>{_inline_format(line)}</p>")
        i += 1

    if in_table:
        html_parts.append("</table>")

    return "\n".join(html_parts)


def _inline_format(text):
    """Convert inline markdown formatting to HTML."""
    import re
    # Bold
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    # Inline code
    text = re.sub(r'`(.+?)`', r'<span class="code">\1</span>', text)
    # Links
    text = re.sub(r'\[(.+?)\]\((.+?)\)', r'<a class="link" href="\2">\1</a>', text)
    return text


def register(mcp):

    @mcp.tool()
    def create_branded_google_doc(
        title: str,
        content: str,
        content_format: str = "markdown",
        author: str = "Clickstream DE Team",
    ) -> str:
        """Create a Grubhub/Wonder branded Google Doc with proper HTML tables and colors.

        Converts markdown content to a beautifully formatted HTML Google Doc with:
        - Grubhub orange (#FF8000) headers and table headers
        - Wonder purple (#7B1FA2) accent callouts
        - Green highlighted rows for good/fixed data
        - Pink highlighted rows for alerts/issues
        - Proper HTML tables with alternating row colors
        - Code blocks, callout boxes, section dividers
        - Clickable links

        Markdown features supported:
        - # ## ### headers
        - | table | rows | (standard markdown tables)
        - **bold**, `code`, [links](url)
        - --- section dividers
        - ``` code blocks ```
        - - bullet points

        Args:
            title: Document title
            content: Document content in markdown (or raw HTML if content_format="html")
            content_format: "markdown" (auto-converts to branded HTML) or "html" (uses as-is with brand CSS prepended)
            author: Author line shown under title (default: Clickstream DE Team)
        """
        try:
            if content_format == "html":
                # Wrap raw HTML with brand CSS
                full_html = f"<html><head>{CSS_TEMPLATE}</head><body>{content}</body></html>"
            else:
                # Convert markdown to branded HTML
                body_html = _md_table_to_html(content)
                full_html = f"""<html><head>{CSS_TEMPLATE}</head><body>
<p style="text-align:right; font-size:9pt; color:#999;">{author}</p>
<h1>{title}</h1>
{body_html}
</body></html>"""

            # Write to temp file and use workspace-mcp CLI to import
            import tempfile
            import os

            with tempfile.NamedTemporaryFile(mode="w", suffix=".html", delete=False) as f:
                f.write(full_html)
                temp_path = f.name

            # Use the Google Workspace MCP indirectly via the import_to_google_doc pattern
            # Since we can't call another MCP tool directly, return the HTML for the caller to use
            os.unlink(temp_path)

            return json.dumps({
                "result": "HTML_READY",
                "title": title,
                "instruction": "Use import_to_google_doc with source_format='html' and this content",
                "html_content": full_html,
                "brand_colors": {
                    "grubhub_orange": GH_ORANGE,
                    "wonder_purple": WONDER_PURPLE,
                    "green_highlight": GREEN_HIGHLIGHT,
                    "light_red_alert": LIGHT_RED_ALERT,
                },
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def convert_md_to_branded_html(
        markdown_content: str,
        title: str = "",
        author: str = "Clickstream DE Team",
    ) -> str:
        """Convert markdown text to Grubhub/Wonder branded HTML ready for Google Docs.

        Takes markdown with tables, headers, code blocks and returns
        complete HTML with brand CSS that can be pasted into import_to_google_doc.

        Args:
            markdown_content: Markdown text to convert
            title: Optional title (added as H1 if provided)
            author: Author line (default: Clickstream DE Team)
        """
        try:
            body_html = _md_table_to_html(markdown_content)

            title_html = ""
            if title:
                title_html = f"""<p style="text-align:right; font-size:9pt; color:#999;">{author}</p>
<h1>{title}</h1>"""

            full_html = f"""<html><head>{CSS_TEMPLATE}</head><body>
{title_html}
{body_html}
</body></html>"""

            return json.dumps({
                "result": "CONVERTED",
                "html": full_html,
                "instruction": "Pass this HTML to import_to_google_doc with source_format='html'",
            })
        except Exception as e:
            return json.dumps({"error": str(e)})