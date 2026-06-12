import base64
from contextlib import redirect_stderr, redirect_stdout
import importlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

import bec.utils.database as database
import bec.utils.ai_strategy_analysis as ai_strategy_analysis
import bec.my_backtesting as my_backtesting
from bec.page_config import configure_page
from bec.my_backtesting import FOLDER_BACKTEST_RESULTS

configure_page()

FOLDER_BACKTEST_RESULTS_URL = getattr(
    my_backtesting, "FOLDER_BACKTEST_RESULTS_URL", "static/backtest_results"
)
FOLDER_BACKTEST_RESULTS_FALLBACK = getattr(
    my_backtesting,
    "FOLDER_BACKTEST_RESULTS_FALLBACK",
    os.path.join(my_backtesting.PROJECT_ROOT, FOLDER_BACKTEST_RESULTS_URL),
)


class StreamlitLogCapture:
    def __init__(self, placeholder, original_stream, max_chars=50000):
        self.placeholder = placeholder
        self.original_stream = original_stream
        self.max_chars = max_chars
        self.buffer = []
        self.last_render_at = 0

    def write(self, text):
        if not text:
            return

        if isinstance(text, bytes):
            text = text.decode("utf-8", errors="replace")
        else:
            text = str(text)

        self.original_stream.write(text)
        self.original_stream.flush()
        self.buffer.append(text)

        now = time.monotonic()
        if "\n" in text or now - self.last_render_at > 0.25:
            self.render()

    def flush(self):
        self.original_stream.flush()
        self.render()

    def render(self):
        output = "".join(self.buffer)[-self.max_chars :]
        self.placeholder.code(
            output or "Waiting for backtest output...", language="text"
        )
        self.last_render_at = time.monotonic()


def get_backtest_filename(row, file_type):
    strategy_id = str(row["Strategy_Id"])
    time_frame = row["Time_Frame"]
    symbol = row["Symbol"]
    return f"{strategy_id} - {time_frame} - {symbol}.{file_type}"


def get_backtest_filename_candidates(row, file_type):
    strategy_id = str(row["Strategy_Id"])
    time_frame = row["Time_Frame"]
    symbol = row["Symbol"]
    strategy_ids = [strategy_id, strategy_id[:12], strategy_id[:11]]
    candidates = []
    for candidate_strategy_id in strategy_ids:
        if not candidate_strategy_id:
            continue
        filename = f"{candidate_strategy_id} - {time_frame} - {symbol}.{file_type}"
        if filename not in candidates:
            candidates.append(filename)
    return candidates


def get_backtest_file_path(row, file_type):
    canonical_path = os.path.join(
        FOLDER_BACKTEST_RESULTS, get_backtest_filename(row, file_type)
    )
    search_dirs = [FOLDER_BACKTEST_RESULTS]
    if FOLDER_BACKTEST_RESULTS_FALLBACK not in search_dirs:
        search_dirs.append(FOLDER_BACKTEST_RESULTS_FALLBACK)

    for folder in search_dirs:
        for filename in get_backtest_filename_candidates(row, file_type):
            file_path = os.path.join(folder, filename)
            if os.path.exists(file_path):
                return file_path
    return canonical_path


def get_backtest_static_url(row, file_type):
    file_path = get_backtest_file_path(row, file_type)
    if os.path.exists(file_path):
        return os.path.join(
            "app", FOLDER_BACKTEST_RESULTS_URL, os.path.basename(file_path)
        )
    return ""


def format_missing_backtest_file_message(row, file_path, file_type):
    search_dirs = [FOLDER_BACKTEST_RESULTS]
    if FOLDER_BACKTEST_RESULTS_FALLBACK not in search_dirs:
        search_dirs.append(FOLDER_BACKTEST_RESULTS_FALLBACK)
    candidates = [
        os.path.join(folder, filename)
        for folder in search_dirs
        for filename in get_backtest_filename_candidates(row, file_type)
    ]
    existing_for_symbol = []
    symbol = str(row["Symbol"])
    for folder in search_dirs:
        try:
            existing_for_symbol.extend(
                os.path.join(folder, filename)
                for filename in os.listdir(folder)
                if symbol in filename and filename.endswith(f".{file_type}")
            )
        except OSError:
            continue
    existing_for_symbol = sorted(existing_for_symbol)

    message = f"{file_type.upper()} report file not found for the selected row.\n\n"
    message += f"Expected path: `{os.path.abspath(file_path)}`"
    if len(candidates) > 1:
        message += "\n\nChecked filename variants:\n"
        message += "\n".join(f"- `{os.path.abspath(candidate)}`" for candidate in candidates)
    if existing_for_symbol:
        message += "\n\nExisting report files for this symbol:\n"
        message += "\n".join(
            f"- `{os.path.abspath(filename)}`" for filename in existing_for_symbol[:20]
        )
    return message


def render_open_html_link(file_path, html_content=None):
    if html_content is None:
        with open(file_path, "r", encoding="utf-8") as file:
            html_content = file.read()
    encoded_html = base64.b64encode(html_content.encode("utf-8")).decode("ascii")

    components.html(
        f"""
        <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined" />
        <a id="open-report" target="_blank" rel="noopener"
           title="Open report in new tab"
           style="display:inline-flex;align-items:center;gap:0.35rem;padding:0.42rem 0.65rem;
                  border:1px solid #d0d7de;border-radius:6px;text-decoration:none;color:#24292f;
                  font-family:sans-serif;font-size:0.95rem;line-height:1;">
            <span class="material-symbols-outlined" style="font-size:18px;line-height:1;">new_window</span>
            Open
        </a>
        <script>
            const htmlBase64 = "{encoded_html}";
            const html = atob(htmlBase64);
            const blob = new Blob([html], {{type: "text/html"}});
            const url = URL.createObjectURL(blob);
            document.getElementById("open-report").href = url;
        </script>
        """,
        height=56,
    )


def quality_report_class(grade):
    grade = str(grade or "").upper()
    return grade if grade in {"A", "B", "C", "D", "F"} else "na"


def get_quality_score_details(row):
    quality_score = {}
    try:
        df = database.get_backtesting_results_by_symbol_timeframe_strategy(
            str(row["Symbol"]),
            str(row["Time_Frame"]),
            str(row["Strategy_Id"]),
        )
        if not df.empty:
            config = json.loads(df.iloc[0].get("Backtest_Config_JSON") or "{}")
            quality_score = config.get("strategy_quality_score_result", {})
    except Exception:
        quality_score = {}

    components = (
        quality_score.get("components", {}) if isinstance(quality_score, dict) else {}
    )
    penalties = (
        quality_score.get("penalties", {}) if isinstance(quality_score, dict) else {}
    )

    def _label(value):
        return str(value).replace("_", " ").title() if value else "n/a"

    return {
        "strongest": (
            _label(max(components, key=components.get)) if components else "n/a"
        ),
        "weakest": _label(min(components, key=components.get)) if components else "n/a",
        "main_penalty": (
            _label(max(penalties, key=penalties.get)) if penalties else "None"
        ),
    }


def build_quality_report_block(row):
    score = row.get("Quality_Score") if hasattr(row, "get") else None
    grade = row.get("Quality_Grade") if hasattr(row, "get") else None
    try:
        score = float(score)
    except (TypeError, ValueError):
        return ""
    if pd.isna(score) or not grade:
        return ""

    grade = str(grade).upper()
    grade_class = quality_report_class(grade)
    details = get_quality_score_details(row)
    colors = {
        "A": ("#ffffff", "#166534"),
        "B": ("#ffffff", "#047857"),
        "C": ("#ffffff", "#854d0e"),
        "D": ("#ffffff", "#9a3412"),
        "F": ("#ffffff", "#991b1b"),
    }
    background, foreground = colors.get(grade, ("#f8fafc", "#172033"))
    return (
        f"<div class='bec-quality-card bec-quality-grade-{grade_class}' "
        f"style='position:relative;display:grid;grid-template-columns:minmax(260px,0.85fr) minmax(360px,1.15fr);align-items:end;gap:24px;width:100%;overflow-x:auto;margin-top:18px;background:{background};border:1px solid #dbe4ee;"
        f"border-left:4px solid {foreground};border-radius:14px;padding:16px 18px;'>"
        "<div style='min-width:0;display:grid;grid-template-columns:minmax(150px,1fr) minmax(86px,auto);gap:20px;align-items:end;'>"
        "<div style='min-width:0;'>"
        "<span style='display:block;color:#64748b;font-size:11px;font-weight:850;letter-spacing:0.13em;text-transform:uppercase;'>Quality Score</span>"
        "<div style='display:flex;align-items:flex-end;gap:0.35rem;min-width:0;margin-top:8px;'>"
        f"<strong style='color:{foreground};font-size:30px;line-height:1;letter-spacing:-0.03em;min-width:0;'>{score:.1f}</strong>"
        f"<small style='color:#64748b;font-size:14px;font-weight:700;letter-spacing:0;line-height:1;min-width:0;'>/ 100</small>"
        "</div>"
        "</div>"
        "<div style='min-width:0;'>"
        "<span style='display:block;color:#64748b;font-size:11px;font-weight:850;letter-spacing:0.13em;text-transform:uppercase;'>Grade</span>"
        f"<strong style='display:block;color:{foreground};font-size:30px;line-height:1;letter-spacing:-0.03em;margin-top:8px;min-width:0;'>{grade}</strong>"
        "</div>"
        "</div>"
        "<div style='min-width:0;display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:18px;align-items:end;'>"
        f"<p style='min-width:0;margin:0;'><span style='display:block;color:#64748b;font-size:11px;font-weight:850;letter-spacing:0.13em;text-transform:uppercase;'>Strongest</span><strong style='display:block;margin-top:6px;color:#172033;font-size:13px;line-height:1.25;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'>{details['strongest']}</strong></p>"
        f"<p style='min-width:0;margin:0;'><span style='display:block;color:#64748b;font-size:11px;font-weight:850;letter-spacing:0.13em;text-transform:uppercase;'>Weakest</span><strong style='display:block;margin-top:6px;color:#172033;font-size:13px;line-height:1.25;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'>{details['weakest']}</strong></p>"
        f"<p style='min-width:0;margin:0;'><span style='display:block;color:#64748b;font-size:11px;font-weight:850;letter-spacing:0.13em;text-transform:uppercase;'>Main Penalty</span><strong style='display:block;margin-top:6px;color:#172033;font-size:13px;line-height:1.25;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'>{details['main_penalty']}</strong></p>"
        "</div>"
        "</div>"
    )


def ensure_quality_score_in_report(html_content, row):
    quality_block = build_quality_report_block(row)
    if not quality_block:
        return html_content

    if "bec-quality-card" in html_content:
        updated_html = re.sub(
            r"<div class='bec-quality-card\b.*?</div>\s*<div class='bec-summary-grid'>",
            quality_block + "<div class='bec-summary-grid'>",
            html_content,
            count=1,
            flags=re.DOTALL,
        )
        if updated_html != html_content:
            return updated_html
        return html_content

    html_content = re.sub(
        r"<div class='bec-quality-score\b.*?</div>\s*</div>",
        "",
        html_content,
        count=1,
        flags=re.DOTALL,
    )

    summary_marker = "<div class='bec-summary-grid'>"
    if summary_marker in html_content:
        return html_content.replace(summary_marker, quality_block + summary_marker, 1)

    title_end = "</div>"
    hero_start = html_content.find("<section class='bec-report-hero'>")
    if hero_start >= 0:
        insert_at = html_content.find(title_end, hero_start)
        if insert_at >= 0:
            insert_at += len(title_end)
            return html_content[:insert_at] + quality_block + html_content[insert_at:]

    return html_content


def ensure_backtest_report_card_layout(html_content):
    if "bec-report-card-layout-v3" in html_content:
        return html_content
    if "bec-performance-grid-top" not in html_content:
        return html_content
    encoded_source_html = base64.b64encode(html_content.encode("utf-8")).decode("ascii")

    css = """
        /* bec-report-card-layout-v3 */
        .bec-performance-grid-top {
            grid-template-columns: 1fr !important;
        }
        .bec-performance-grid-secondary {
            grid-template-columns: repeat(6, minmax(0, 1fr)) !important;
        }
        .bec-quality-dial {
            grid-column: auto !important;
        }
        .bec-quality-breakdown p {
            grid-template-columns: minmax(104px, max-content) minmax(0, 1fr) !important;
        }
        .bec-quality-breakdown strong {
            min-width: 0;
            white-space: normal !important;
            overflow-wrap: anywhere;
        }
        .bec-theme-toggle {
            display: inline-flex !important;
            align-items: center !important;
            justify-content: center !important;
            width: 36px !important;
            height: 36px !important;
            border-radius: 999px !important;
            font-size: 18px !important;
            line-height: 1 !important;
            padding: 0 !important;
        }
        .bec-report-actions {
            display: flex !important;
            align-items: center !important;
            gap: 8px !important;
        }
        .bec-share-menu {
            position: relative !important;
        }
        .bec-share-toggle {
            display: inline-flex !important;
            align-items: center !important;
            justify-content: center !important;
            width: 36px !important;
            height: 36px !important;
            border: 1px solid var(--bec-border, #dbe4ee) !important;
            border-radius: 999px !important;
            background: var(--bec-surface-soft, #f8fafc) !important;
            color: var(--bec-text, #172033) !important;
            cursor: pointer !important;
            font-size: 18px !important;
            font-weight: 850 !important;
            line-height: 1 !important;
            padding: 0 !important;
        }
        .bec-share-toggle:hover,
        .bec-theme-toggle:hover {
            border-color: var(--bec-blue, #2563eb) !important;
            color: var(--bec-blue, #2563eb) !important;
        }
        .bec-share-toggle .material-symbols-outlined {
            font-size: 20px !important;
            font-variation-settings: "FILL" 0, "wght" 400, "GRAD" 0, "opsz" 24 !important;
            line-height: 1 !important;
        }
        .bec-theme-toggle .material-symbols-outlined {
            font-size: 20px !important;
            font-variation-settings: "FILL" 0, "wght" 400, "GRAD" 0, "opsz" 24 !important;
            line-height: 1 !important;
        }
        .bec-share-options {
            position: absolute !important;
            top: calc(100% + 8px) !important;
            right: 0 !important;
            z-index: 20 !important;
            display: none !important;
            min-width: 160px !important;
            overflow: hidden !important;
            border: 1px solid var(--bec-border, #dbe4ee) !important;
            border-radius: 10px !important;
            background: var(--bec-surface, #ffffff) !important;
            box-shadow: 0 14px 32px rgba(15, 23, 42, 0.14) !important;
        }
        .bec-share-menu.is-open .bec-share-options {
            display: block !important;
        }
        .bec-share-menu:focus-within .bec-share-options {
            display: block !important;
        }
        .bec-share-options button {
            display: block !important;
            width: 100% !important;
            border: 0 !important;
            background: transparent !important;
            color: var(--bec-text, #172033) !important;
            cursor: pointer !important;
            font-size: 12px !important;
            font-weight: 750 !important;
            padding: 10px 12px !important;
            text-align: left !important;
            white-space: nowrap !important;
        }
        .bec-share-options button:hover {
            background: var(--bec-surface-soft, #f8fafc) !important;
            color: var(--bec-blue, #2563eb) !important;
        }
        @media print {
            @page {
                size: landscape;
                margin: 10mm;
            }
            * {
                -webkit-print-color-adjust: exact !important;
                print-color-adjust: exact !important;
            }
            .bec-report-actions,
            .bec-trades-toolbar,
            .dt-search,
            .dataTables_filter,
            .dt-length,
            .dt-info,
            .dt-paging,
            .dataTables_length,
            .dataTables_info,
            .dataTables_paginate {
                display: none !important;
            }
            body {
                background: #ffffff !important;
            }
            .bec-report-shell,
            .bec-panel,
            .bec-chart-card {
                box-shadow: none !important;
                break-inside: avoid;
            }
        }
        @media screen and (max-width: 1000px) {
            .bec-performance-grid-secondary {
                grid-template-columns: 1fr !important;
            }
        }
    """
    script = """
        <script>
            window.__becReportSourceHtmlBase64 = "__BEC_REPORT_SOURCE_HTML_BASE64__";
            window.addEventListener("DOMContentLoaded", function () {
                const topGrid = document.querySelector(".bec-performance-grid-top");
                const metricGrid = document.querySelector(".bec-performance-grid-secondary");
                if (topGrid && metricGrid) {
                    Array.from(topGrid.querySelectorAll(".bec-perf-card")).reverse().forEach(function (card) {
                        metricGrid.insertBefore(card, metricGrid.firstChild);
                    });
                }

                function simplifyThemeButtons() {
                    const theme = document.documentElement.getAttribute("data-bec-theme") || "light";
                    document.querySelectorAll("[data-theme-toggle]").forEach(function (button) {
                        button.innerHTML = `<span class="material-symbols-outlined">${theme === "dark" ? "light_mode" : "dark_mode"}</span>`;
                        button.setAttribute("aria-label", theme === "dark" ? "Switch to light theme" : "Switch to dark theme");
                        button.setAttribute("title", theme === "dark" ? "Switch to light theme" : "Switch to dark theme");
                    });
                }
                simplifyThemeButtons();
                document.querySelectorAll("[data-theme-toggle]").forEach(function (button) {
                    button.addEventListener("click", function () {
                        window.setTimeout(simplifyThemeButtons, 0);
                    });
                });

                function ensureShareActions() {
                    const header = document.querySelector(".bec-topbar");
                    if (!header) return;
                    let actions = header.querySelector(".bec-report-actions");
                    const themeButton = header.querySelector("[data-theme-toggle]");
                    if (!actions) {
                        actions = document.createElement("div");
                        actions.className = "bec-report-actions";
                        if (themeButton) {
                            header.insertBefore(actions, themeButton);
                            actions.appendChild(themeButton);
                        } else {
                            header.appendChild(actions);
                        }
                    }
                    if (!actions.querySelector("[data-share-toggle]")) {
                        const shareMenu = document.createElement("div");
                        shareMenu.className = "bec-share-menu";
                        shareMenu.innerHTML = `
                            <button class="bec-share-toggle" type="button" data-share-toggle aria-haspopup="true" aria-expanded="false" aria-label="Share report" title="Share report"><span class="material-symbols-outlined">share</span></button>
                            <div class="bec-share-options" data-share-options>
                                <button type="button" data-download-html>Download HTML</button>
                                <button type="button" data-export-pdf>Export PDF</button>
                            </div>
                        `;
                        actions.insertBefore(shareMenu, actions.firstChild);
                    }
                }

                function reportFilename(extension) {
                    const reportTitle = document.querySelector(".bec-topbar h1")?.textContent?.trim() || "strategy";
                    const reportSubtitle = Array.from(document.querySelectorAll(".bec-subtitle strong, .bec-subtitle span"))
                        .map(element => element.textContent.trim())
                        .filter(Boolean)
                        .join("-");
                    return `${reportTitle}-${reportSubtitle || "report"}.${extension}`
                        .replace(/[^a-z0-9._-]+/gi, "_")
                        .replace(/^_+|_+$/g, "");
                }

                function downloadReportHtml() {
                    const html = window.__becReportSourceHtmlBase64
                        ? atob(window.__becReportSourceHtmlBase64)
                        : "<!doctype html>\\n" + document.documentElement.outerHTML;
                    const filename = reportFilename("html");
                    try {
                        const blob = new Blob([html], {type: "text/html;charset=utf-8;"});
                        const url = URL.createObjectURL(blob);
                        if (window.self !== window.top) {
                            window.open(url, "_blank", "noopener");
                            window.setTimeout(function () {
                                URL.revokeObjectURL(url);
                            }, 30000);
                            return;
                        }
                        const link = document.createElement("a");
                        link.href = url;
                        link.download = filename;
                        link.target = "_blank";
                        document.body.appendChild(link);
                        link.click();
                        link.remove();
                        window.setTimeout(function () {
                            URL.revokeObjectURL(url);
                        }, 1000);
                    } catch (error) {
                        const fallbackUrl = "data:text/html;charset=utf-8," + encodeURIComponent(html);
                        window.open(fallbackUrl, "_blank", "noopener");
                    }
                }

                ensureShareActions();
                if (!window.__becReportEmbeddedDownloadBound) {
                    window.__becReportEmbeddedDownloadBound = true;
                    document.addEventListener("click", function (event) {
                        const htmlDownloadButton = event.target.closest("[data-download-html]");
                        if (!htmlDownloadButton || window.self === window.top) return;
                        event.preventDefault();
                        event.stopPropagation();
                        event.stopImmediatePropagation();
                        htmlDownloadButton.closest(".bec-share-menu")?.classList.remove("is-open");
                        downloadReportHtml();
                    }, true);
                }
                if (!window.__becReportShareBound) {
                    window.__becReportShareBound = true;
                    document.addEventListener("click", function (event) {
                        const shareToggle = event.target.closest("[data-share-toggle]");
                        if (shareToggle) {
                            event.stopPropagation();
                            const menu = shareToggle.closest(".bec-share-menu");
                            const isOpen = menu?.classList.toggle("is-open");
                            shareToggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
                            return;
                        }

                        const htmlDownloadButton = event.target.closest("[data-download-html]");
                        if (htmlDownloadButton) {
                            htmlDownloadButton.closest(".bec-share-menu")?.classList.remove("is-open");
                            downloadReportHtml();
                            return;
                        }

                        const pdfExportButton = event.target.closest("[data-export-pdf]");
                        if (pdfExportButton) {
                            pdfExportButton.closest(".bec-share-menu")?.classList.remove("is-open");
                            window.print();
                            return;
                        }

                        document.querySelectorAll(".bec-share-menu.is-open").forEach(function (menu) {
                            menu.classList.remove("is-open");
                            menu.querySelector("[data-share-toggle]")?.setAttribute("aria-expanded", "false");
                        });
                    });
                }
            });
        </script>
    """.replace("__BEC_REPORT_SOURCE_HTML_BASE64__", encoded_source_html)

    material_symbols_link = '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200&icon_names=dark_mode,light_mode,share" />'
    if "Material+Symbols+Outlined" not in html_content:
        if "</head>" in html_content:
            html_content = html_content.replace("</head>", material_symbols_link + "</head>", 1)
        else:
            html_content = material_symbols_link + html_content

    if "</style>" in html_content:
        html_content = html_content.replace("</style>", css + "\n</style>", 1)
    elif "</head>" in html_content:
        html_content = html_content.replace("</head>", f"<style>{css}</style></head>", 1)
    else:
        html_content = f"<style>{css}</style>" + html_content

    if "</body>" in html_content:
        return html_content.replace("</body>", script + "\n</body>", 1)
    return html_content + script


def ensure_trades_csv_download_in_report(html_content):
    if 'id="trades-table"' not in html_content:
        return html_content
    if (
        "bec-trades-toolbar-v2" in html_content
        or (
            "function downloadTradesCsv()" in html_content
            and "new DataTable(\"#trades-table\"" in html_content
        )
    ):
        return html_content

    css = """
        .bec-trades-toolbar {
            display: flex;
            align-items: center;
            justify-content: flex-end;
            gap: 8px;
            flex-wrap: wrap;
            margin: 0 0 12px 0;
        }
        .bec-trades-download {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-height: 32px;
            margin-right: 10px;
            border: 1px solid var(--bec-border, #dbe4ee);
            border-radius: 8px;
            background: var(--bec-surface-soft, #f8fafc);
            color: var(--bec-text, #172033);
            cursor: pointer;
            font-size: 12px;
            font-weight: 800;
            line-height: 1;
            padding: 8px 11px;
            white-space: nowrap;
        }
        .bec-trades-download:hover {
            border-color: var(--bec-blue, #2563eb);
            color: var(--bec-blue, #2563eb);
        }
        .bec-trades-search {
            min-height: 32px;
            border: 1px solid var(--bec-border, #dbe4ee);
            border-radius: 8px;
            background: var(--bec-surface, #ffffff);
            color: var(--bec-text, #172033);
            font-size: 12px;
            padding: 7px 10px;
            width: min(260px, 100%);
        }
        .dt-search,
        .dataTables_filter {
            display: none !important;
        }
    """
    script = """
        <!-- bec-trades-toolbar-v2 -->
        <script>
            window.addEventListener("DOMContentLoaded", function () {
                const table = document.querySelector("#trades-table");
                if (!table) return;

                const headers = Array.from(table.querySelectorAll("thead th")).map(th => th.textContent.trim());
                const tradeRows = Array.from(table.querySelectorAll("tbody tr")).map(row =>
                    Array.from(row.querySelectorAll("td")).map(cell => cell.textContent.trim())
                );
                const reportTitle = document.querySelector(".bec-topbar h1")?.textContent?.trim() || "strategy";
                const reportSubtitle = Array.from(document.querySelectorAll(".bec-subtitle strong, .bec-subtitle span"))
                    .map(element => element.textContent.trim())
                    .filter(Boolean)
                    .join("-");
                const csvFilename = `${reportTitle}-${reportSubtitle || "trades"}-trades.csv`
                    .replace(/[^a-z0-9._-]+/gi, "_")
                    .replace(/^_+|_+$/g, "");

                function csvEscape(value) {
                    const text = String(value ?? "");
                    if (
                        text.includes(",")
                        || text.includes('"')
                        || text.includes(String.fromCharCode(10))
                        || text.includes(String.fromCharCode(13))
                    ) {
                        return `"${text.replace(/"/g, '""')}"`;
                    }
                    return text;
                }

                function downloadTradesCsv() {
                    const csvRows = [headers, ...tradeRows].map(row => row.map(csvEscape).join(","));
                    const blob = new Blob([csvRows.join("\\n")], {type: "text/csv;charset=utf-8;"});
                    const url = URL.createObjectURL(blob);
                    const link = document.createElement("a");
                    link.href = url;
                    link.download = csvFilename;
                    document.body.appendChild(link);
                    link.click();
                    link.remove();
                    URL.revokeObjectURL(url);
                }

                let tradesDataTable = null;
                if (window.DataTable && !document.querySelector("#trades-table_wrapper")) {
                    try {
                        tradesDataTable = new DataTable("#trades-table", {
                            pageLength: 25,
                            order: [[1, "asc"]],
                            scrollX: true,
                            rowCallback: function(row, data) {
                                const returnIndex = headers.indexOf("Return_Pct");
                                if (returnIndex >= 0 && parseFloat(data[returnIndex]) < 0) {
                                    row.classList.add("bec-loss-row");
                                }
                            }
                        });
                    } catch (error) {
                        tradesDataTable = null;
                    }
                } else if (window.DataTable && document.querySelector("#trades-table_wrapper")) {
                    try {
                        tradesDataTable = new DataTable.Api("#trades-table");
                    } catch (error) {
                        tradesDataTable = null;
                    }
                }

                if (!document.querySelector(".bec-trades-toolbar")) {
                    const toolbar = document.createElement("div");
                    toolbar.className = "bec-trades-toolbar";

                    const searchInput = document.createElement("input");
                    searchInput.type = "search";
                    searchInput.className = "bec-trades-search";
                    searchInput.placeholder = "Search trades";
                    searchInput.setAttribute("aria-label", "Search trades");

                    const button = document.createElement("button");
                    button.type = "button";
                    button.className = "bec-trades-download";
                    button.textContent = "Download CSV";
                    button.addEventListener("click", downloadTradesCsv);

                    searchInput.addEventListener("input", function () {
                        const query = searchInput.value.trim().toLowerCase();
                        if (tradesDataTable) {
                            tradesDataTable.search(query).draw();
                            return;
                        }

                        Array.from(table.querySelectorAll("tbody tr")).forEach(row => {
                            row.style.display = row.textContent.toLowerCase().includes(query) ? "" : "none";
                        });
                    });

                    toolbar.appendChild(button);
                    toolbar.appendChild(searchInput);
                    const wrapper = document.querySelector("#trades-table_wrapper");
                    const anchor = wrapper || table;
                    anchor.parentNode.insertBefore(toolbar, anchor);
                }
            });
        </script>
    """

    if "</style>" in html_content:
        html_content = html_content.replace("</style>", css + "\n</style>", 1)
    elif "</head>" in html_content:
        html_content = html_content.replace("</head>", f"<style>{css}</style></head>", 1)

    if "</body>" in html_content:
        return html_content.replace("</body>", script + "\n</body>", 1)

    return html_content + script


def estimate_backtest_report_height(html_content):
    trades_rows = html_content.count("<tr")
    has_trades_table = 'id="trades-table"' in html_content

    # Streamlit components are iframes with fixed height. Use a generous estimate
    # to avoid nested scrolling inside the embedded report.
    base_height = 3200
    trades_height = min(max(trades_rows - 1, 0), 25) * 28 if has_trades_table else 0
    return min(max(base_height + trades_height, 3400), 5600)


def run_backtest_for_selection(strategy_id, symbol, timeframe):
    strategy_id = str(strategy_id).strip()
    symbol = str(symbol).strip().upper()
    timeframe = str(timeframe).strip()
    optimize = get_strategy_backtest_optimize(strategy_id)

    strategy_module = importlib.import_module("bec.my_backtesting")
    strategy_impl = (
        strategy_module.resolve_strategy(strategy_id)
        if hasattr(strategy_module, "resolve_strategy")
        else getattr(strategy_module, strategy_id, None)
    )
    if strategy_impl is None:
        st.error(f"Strategy '{strategy_id}' is not available.")
        return False

    status_label = f"Running backtest: {strategy_id} - {symbol} - {timeframe} (optimize={optimize})"
    with st.status(status_label, expanded=True) as status:
        log_placeholder = st.empty()
        backtesting_script = os.path.abspath(my_backtesting.__file__)
        project_root = os.path.dirname(os.path.dirname(backtesting_script))
        command = [
            sys.executable,
            "-m",
            "bec.my_backtesting",
            "--symbol",
            symbol,
            "--timeframe",
            timeframe,
            "--strategy",
            strategy_id,
        ]
        if optimize:
            command.append("--optimize")
        process = subprocess.Popen(
            command,
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        output = []
        for line in process.stdout:
            output.append(line)
            log_placeholder.code(
                "".join(output)[-50000:] or "Waiting for backtest output...",
                language="text",
            )

        return_code = process.wait()
        result = return_code == 0

        if result:
            status.update(
                label="Backtest finished. Results were updated.", state="complete"
            )
        else:
            status.update(
                label="Backtest failed. Check the output above.", state="error"
            )

    if result:
        # st.success("Backtest finished. Results were updated.")
        return True

    st.error("Backtest failed. Check the app logs for details.")
    return False


def run_selected_backtest(row):
    return run_backtest_for_selection(
        strategy_id=str(row["Strategy_Id"]),
        symbol=str(row["Symbol"]),
        timeframe=str(row["Time_Frame"]),
    )


def build_selected_backtest_row(strategy_id, symbol, timeframe):
    return pd.Series(
        {
            "Strategy_Id": str(strategy_id),
            "Strategy_Name": format_func_strategies(str(strategy_id)),
            "Symbol": str(symbol).strip().upper(),
            "Time_Frame": str(timeframe).strip(),
        }
    )


def get_strategy_backtest_optimize(strategy_id):
    df_strategy = database.get_strategy_by_id(str(strategy_id).strip())
    if df_strategy.empty:
        return False
    try:
        definition = database.get_strategy_definition(str(strategy_id).strip())
        parameters = definition.get("parameters", {}) if isinstance(definition, dict) else {}
        if isinstance(parameters, dict) and any(
            bool(spec.get("optimizable", False)) for spec in parameters.values() if isinstance(spec, dict)
        ):
            return True
    except Exception:
        pass
    return bool(int(df_strategy.iloc[0]["Backtest_Optimize"]))


def build_backtesting_job(row):
    return {
        "strategy_id": str(row["Strategy_Id"]).strip(),
        "symbol": str(row["Symbol"]).strip().upper(),
        "timeframe": str(row["Time_Frame"]).strip(),
        "optimize": get_strategy_backtest_optimize(row["Strategy_Id"]),
    }


def enqueue_backtesting_rows(rows):
    jobs = [build_backtesting_job(row) for row in rows]
    return database.enqueue_backtesting_jobs(jobs)


def get_optimization_combination_warnings(rows):
    settings = database.get_backtesting_settings()
    max_combinations = int(settings.get("Optimization_Max_Combinations", 300))
    maximize = str(settings.get("Maximize", "SQN"))
    warnings = []
    for row in rows:
        strategy_id = str(row["Strategy_Id"]).strip()
        if not get_strategy_backtest_optimize(strategy_id):
            continue
        try:
            definition = database.get_strategy_definition(strategy_id)
            combination_count, optimized_names = (
                my_backtesting.count_declarative_optimization_combinations(
                    definition,
                    maximize,
                )
            )
        except Exception:
            continue
        if optimized_names and combination_count > max_combinations:
            warnings.append(
                {
                    "Strategy": format_func_strategies(strategy_id),
                    "Strategy_Id": strategy_id,
                    "Symbol": str(row["Symbol"]).strip().upper(),
                    "Time_Frame": str(row["Time_Frame"]).strip(),
                    "Combinations": int(combination_count),
                    "Maximum": int(max_combinations),
                }
            )
    return warnings


def queue_backtesting_rows_with_message(rows):
    enqueue_result = enqueue_backtesting_rows(rows)
    queued_count = len(enqueue_result["queued"])
    skipped_count = len(enqueue_result["skipped"])
    if queued_count:
        first_job = enqueue_result["queued"][0]
        st.session_state["bt_results_pending_selection"] = {
            "strategy_id": str(first_job["strategy_id"]),
            "symbol": str(first_job["symbol"]),
            "timeframe": str(first_job["timeframe"]),
        }
        st.session_state["bt_results_queue_message"] = (
            "success",
            f"Queued {queued_count} backtesting job(s). "
            f"Skipped {skipped_count} already queued/running job(s).",
        )
    elif skipped_count:
        st.session_state["bt_results_queue_message"] = (
            "info",
            "All selected backtests are already queued or running.",
        )


@st.dialog("Large optimization")
def confirm_large_optimization_dialog(rows, warnings):
    st.write(
        "Some selected backtests define more parameter combinations than the "
        "configured overfitting alert threshold. They will still run with the full "
        "native optimizer if you continue."
    )
    st.dataframe(
        pd.DataFrame(warnings),
        hide_index=True,
        width="content",
        column_config={
            "Strategy_Id": None,
            "Combinations": st.column_config.NumberColumn("Combinations", format="%d"),
            "Maximum": st.column_config.NumberColumn("Maximum", format="%d"),
            "Time_Frame": st.column_config.TextColumn("TF"),
        },
    )
    actions = st.container(horizontal=True)
    if actions.button("Continue", type="primary", icon=":material/play_arrow:"):
        queue_backtesting_rows_with_message(rows)
        st.session_state.pop("bt_results_pending_large_optimization_rows", None)
        st.session_state.pop("bt_results_pending_large_optimization_warnings", None)
        st.rerun()
    if actions.button("Cancel", icon=":material/cancel:"):
        st.session_state.pop("bt_results_pending_large_optimization_rows", None)
        st.session_state.pop("bt_results_pending_large_optimization_warnings", None)
        st.rerun()


def get_static_log_url(log_path):
    if not log_path:
        return ""
    normalized = str(log_path).replace("\\", "/")
    if os.path.exists(normalized):
        return os.path.join("app", normalized)
    return ""


def tail_text_file(file_path, max_chars=12000):
    if not file_path or not os.path.exists(file_path):
        return ""
    with open(file_path, "r", encoding="utf-8", errors="replace") as file:
        content = file.read()
    return content[-max_chars:]


def style_backtesting_job_status(value):
    status = str(value or "").strip().lower()
    colors = {
        "queued": "#7c3aed",
        "pending": "#7c3aed",
        "running": "#2563eb",
        "completed": "#16a34a",
        "failed": "#dc2626",
        "cancelled": "#64748b",
        "canceled": "#64748b",
        "skipped": "#ca8a04",
        "unknown": "#475569",
    }
    color = colors.get(status, "#475569")
    return f"color: {color};"


def format_backtesting_job_timestamp(value):
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return ""
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def format_backtesting_job_duration(row):
    started_at = pd.to_datetime(row.get("started_at"), errors="coerce", utc=True)
    if pd.isna(started_at):
        return ""

    finished_at = pd.to_datetime(row.get("finished_at"), errors="coerce", utc=True)
    if pd.isna(finished_at) and str(row.get("status", "")).strip().lower() == "running":
        finished_at = pd.Timestamp.utcnow()
    if pd.isna(finished_at):
        return ""

    elapsed = finished_at - started_at
    if elapsed.total_seconds() < 0:
        return ""

    total_seconds = int(elapsed.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours >= 24:
        days, hours = divmod(hours, 24)
        return f"{days}d {hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


@st.fragment(run_every=3)
def render_backtesting_jobs_status():
    counts = database.get_backtesting_job_counts()
    count_map = (
        dict(zip(counts["status"], counts["count"])) if not counts.empty else {}
    )
    jobs = database.get_backtesting_jobs(limit=50)
    queued = int(count_map.get("queued", 0))
    running = int(count_map.get("running", 0))
    completed = int(count_map.get("completed", 0))
    failed = int(count_map.get("failed", 0))
    queue_summary = (
        f"Backtesting Queue: {queued} queued | {running} running | "
        f"{completed} completed | {failed} failed"
    )

    st.subheader("Backtesting Queue")

    if jobs.empty:
        st.caption(queue_summary.replace("Backtesting Queue: ", ""))
        return

    running_jobs = jobs[jobs["status"] == "running"]
    if not running_jobs.empty:
        current_job = running_jobs.iloc[0]
        st.status(
            f"Running backtest job #{current_job['id']}: "
            f"{current_job['strategy_id']} - {current_job['symbol']} - {current_job['timeframe']}",
            state="running",
            expanded=False,
        )
    elif queued > 0:
        st.status(
            f"Waiting for jobs_runner to start {queued} queued backtesting job(s)...",
            state="running",
            expanded=False,
        )

    has_active_jobs = queued > 0 or running > 0 or failed > 0
    with st.expander(queue_summary.replace("Backtesting Queue: ", ""), expanded=has_active_jobs):
        progress_batch_id = None
        if not running_jobs.empty:
            progress_batch_id = str(running_jobs.iloc[0]["batch_id"])
        elif queued > 0:
            queued_jobs = jobs[jobs["status"] == "queued"].copy()
            if not queued_jobs.empty:
                queued_jobs = queued_jobs.sort_values(["created_at", "id"], ascending=[True, True])
                progress_batch_id = str(queued_jobs.iloc[0]["batch_id"])

        if progress_batch_id:
            batch_counts = database.get_backtesting_job_counts_by_batch(progress_batch_id)
            batch_count_map = (
                dict(zip(batch_counts["status"], batch_counts["count"]))
                if not batch_counts.empty
                else {}
            )
            progress_queued = int(batch_count_map.get("queued", 0))
            progress_running = int(batch_count_map.get("running", 0))
            progress_completed = int(batch_count_map.get("completed", 0))
            progress_failed = int(batch_count_map.get("failed", 0))
        else:
            progress_queued = queued
            progress_running = running
            progress_completed = completed
            progress_failed = failed

        total_jobs = progress_queued + progress_running + progress_completed + progress_failed
        processed_jobs = progress_completed + progress_failed
        progress_value = processed_jobs / total_jobs if total_jobs else 0
        progress_label = (
            f"{processed_jobs}/{total_jobs} processed "
            f"({progress_value:.0%})"
        )
        if progress_batch_id:
            progress_label = f"{progress_label} for current batch"
        st.progress(
            progress_value,
            text=progress_label,
        )
        st.caption("Most recent 50 queued, running and completed backtesting jobs.")

        jobs_display = jobs.copy()
        jobs_display["Target"] = (
            jobs_display["strategy_id"].astype(str)
            + " - "
            + jobs_display["symbol"].astype(str)
            + " - "
            + jobs_display["timeframe"].astype(str)
        )
        jobs_display["optimize"] = jobs_display["optimize"].astype(bool)
        jobs_display["Log"] = jobs_display["log_path"].apply(get_static_log_url)
        jobs_display["Duration"] = jobs_display.apply(format_backtesting_job_duration, axis=1)
        for timestamp_column in ("created_at", "started_at", "finished_at"):
            jobs_display[timestamp_column] = jobs_display[timestamp_column].apply(format_backtesting_job_timestamp)
        jobs_display = jobs_display[
            [
                "id",
                "batch_id",
                "Target",
                "optimize",
                "status",
                "Duration",
                "created_at",
                "started_at",
                "finished_at",
                "return_code",
                "Log",
                "error_message",
            ]
        ]

        styled_jobs_display = jobs_display.style.map(
            style_backtesting_job_status,
            subset=["status"],
        )

        st.dataframe(
            styled_jobs_display,
            width="content",
            hide_index=True,
            height=260,
            column_config={
                "id": st.column_config.NumberColumn("Job", format="%d"),
                "batch_id": st.column_config.TextColumn("Batch"),
                "Target": st.column_config.TextColumn("Backtest"),
                "optimize": st.column_config.CheckboxColumn("Optimize"),
                "status": st.column_config.TextColumn("Status"),
                "Duration": st.column_config.TextColumn("Duration"),
                "created_at": st.column_config.TextColumn("Created"),
                "started_at": st.column_config.TextColumn("Started"),
                "finished_at": st.column_config.TextColumn("Finished"),
                "return_code": st.column_config.NumberColumn("Return", format="%d"),
                "Log": st.column_config.LinkColumn("Log", display_text="Open"),
                "error_message": st.column_config.TextColumn("Error"),
            },
        )

        active_jobs = jobs[jobs["status"].isin(["running", "failed"])].copy()
        active_jobs = active_jobs[active_jobs["log_path"].fillna("") != ""]
        if not active_jobs.empty:
            latest_job = active_jobs.iloc[0]
            with st.expander(f"Latest job log: #{latest_job['id']} {latest_job['status']}", expanded=False):
                st.code(
                    tail_text_file(str(latest_job["log_path"]))
                    or "Log file not available yet.",
                    language="text",
                )


def build_ai_analysis_export_payload(row, model, selected_csv_path, context, analysis):
    return {
        "exported_at": datetime.now().isoformat(timespec="seconds"),
        "openai_model": model,
        "source_csv": os.path.abspath(selected_csv_path),
        "strategy": {
            "id": str(row["Strategy_Id"]),
            "name": str(row.get("Strategy_Name", row["Strategy_Id"])),
            "symbol": str(row["Symbol"]),
            "timeframe": str(row["Time_Frame"]),
        },
        "context_sent_to_openai": context,
        "raw_ai_response_json": analysis,
    }


def build_ai_analysis_export_filename(row):
    strategy_id = str(row["Strategy_Id"]).strip()
    symbol = str(row["Symbol"]).strip().upper()
    timeframe = str(row["Time_Frame"]).strip()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"ai_strategy_analysis - {strategy_id} - {timeframe} - {symbol} - {timestamp}.json"


def render_ai_strategy_analysis(analysis, export_payload=None, export_filename=None):
    quality_score = ((export_payload or {}).get("context_sent_to_openai") or {}).get(
        "strategy_quality_score"
    )
    if quality_score:
        st.markdown("#### Strategy Quality Score")
        st.metric(
            "Score",
            f"{float(quality_score.get('score', 0)):.1f}/100",
            delta=f"Grade {quality_score.get('grade', 'n/a')}",
        )
        components = quality_score.get("components", {})
        if components:
            st.dataframe(
                pd.DataFrame(
                    [
                        {"Component": key.replace("_", " ").title(), "Score": value}
                        for key, value in components.items()
                    ]
                ),
                width="content",
                hide_index=True,
            )
        penalties = quality_score.get("penalties", {})
        if penalties:
            st.caption(quality_score.get("summary", ""))

    st.markdown("#### Summary")
    st.write(analysis.get("summary", ""))

    findings = analysis.get("main_findings", [])
    if findings:
        st.markdown("#### Main Findings")
        st.dataframe(pd.DataFrame(findings), width="content", hide_index=True)

    risk_assessment = analysis.get("risk_assessment", {})
    if risk_assessment:
        st.markdown("#### Risk Assessment")
        risk_rows = [
            {"Area": key.replace("_", " ").title(), "Comment": value}
            for key, value in risk_assessment.items()
        ]
        st.dataframe(pd.DataFrame(risk_rows), width="content", hide_index=True)

    recommended_tests = analysis.get("recommended_tests", [])
    if recommended_tests:
        st.markdown("#### Recommended Tests")
        st.dataframe(pd.DataFrame(recommended_tests), width="content", hide_index=True)

    data_quality_notes = analysis.get("data_quality_notes", [])
    if data_quality_notes:
        st.markdown("#### Data Quality Notes")
        for note in data_quality_notes:
            st.write(f"- {note}")

    st.markdown("#### Final Recommendation")
    st.write(analysis.get("final_recommendation", ""))

    with st.expander("Raw AI response JSON"):
        st.json(analysis)

    if export_payload:
        export_json = json.dumps(
            export_payload, ensure_ascii=False, indent=2, default=str
        )
        st.download_button(
            label="Download analysis JSON",
            data=export_json,
            file_name=export_filename or "ai_strategy_analysis.json",
            mime="application/json",
            icon=":material/download:",
            help="Downloads the context sent to OpenAI plus the raw AI response in one file.",
        )


def render_ai_strategy_analysis_controls(row):
    selected_csv_path = get_backtest_file_path(row, "csv")

    st.subheader("AI Strategy Analysis")
    st.caption(
        "Uses OpenAI to analyze the selected backtest from STATS, CONFIG and representative trades."
    )

    if not os.path.exists(selected_csv_path):
        st.warning(
            "CSV report file not found for the selected row. Run the backtest first."
        )
        return

    # col_model, col_button = st.columns([0.35, 0.65])
    cont = st.container(horizontal=True, vertical_alignment="bottom")
    with cont:
        model_options = ["gpt-5.5", "gpt-5.4", "gpt-5.4-mini"]
        configured_model = os.getenv(
            "BEC_OPENAI_MODEL", ai_strategy_analysis.DEFAULT_MODEL
        )
        model_index = (
            model_options.index(configured_model)
            if configured_model in model_options
            else None
        )
        model = st.selectbox(
            "OpenAI model",
            options=model_options,
            index=model_index,
            placeholder=configured_model,
            accept_new_options=True,
            width=250,
            help="Use GPT-5.4-mini for faster/cheaper analysis, or type a different model ID.",
            key="ai_strategy_model",
        )
        model = model or configured_model

    config_error = None
    try:
        ai_strategy_analysis.validate_openai_configuration(model=model)
    except RuntimeError as exc:
        config_error = str(exc)

    with cont:
        analyze = st.button(
            "Analyze", key="analyze_selected_backtest", icon=":material/cognition_2:"
        )

    if config_error:
        st.warning(config_error)

    cache_key = (
        f"{row['Strategy_Id']}|{row['Symbol']}|{row['Time_Frame']}|"
        f"{os.path.getmtime(selected_csv_path)}|{model}"
    )

    if analyze:
        if config_error:
            st.error(config_error)
            return
        try:
            with st.status(
                "Preparing backtest context for AI analysis...", expanded=True
            ) as status:
                context = ai_strategy_analysis.build_backtest_analysis_context(
                    row, selected_csv_path
                )
                status.write(
                    f"Prepared context with {context['trade_summary']['count_total']} trades."
                )
                status.update(label="Calling OpenAI...", state="running")
                analysis = ai_strategy_analysis.analyze_backtest_with_openai(
                    context,
                    model=model,
                    timeout=180,
                )
                st.session_state["last_ai_strategy_analysis_key"] = cache_key
                st.session_state["last_ai_strategy_analysis"] = analysis
                st.session_state["last_ai_strategy_analysis_context"] = context
                status.update(label="AI analysis completed.", state="complete")
        except Exception as exc:
            st.error(f"AI analysis failed: {exc}")
            return

    if st.session_state.get("last_ai_strategy_analysis_key") == cache_key:
        analysis = st.session_state["last_ai_strategy_analysis"]
        context = st.session_state.get("last_ai_strategy_analysis_context")
        export_payload = None
        if context:
            export_payload = build_ai_analysis_export_payload(
                row=row,
                model=model,
                selected_csv_path=selected_csv_path,
                context=context,
                analysis=analysis,
            )
        render_ai_strategy_analysis(
            analysis,
            export_payload=export_payload,
            export_filename=build_ai_analysis_export_filename(row),
        )


st.markdown("## Backtesting Results")

df_strategies = database.get_all_strategies()
dict_strategies = (
    dict(zip(df_strategies["Id"], df_strategies["Name"]))
    if not df_strategies.empty
    else {}
)


def format_func_strategies(option):
    return dict_strategies.get(option, option)


def restore_filter_widget(state_key, widget_key, default):
    if widget_key not in st.session_state:
        st.session_state[widget_key] = st.session_state.get(state_key, default)


def persist_filter_widget(state_key, widget_key):
    st.session_state[state_key] = st.session_state.get(widget_key)


def migrate_filter_state(old_key, new_key):
    if new_key not in st.session_state and old_key in st.session_state:
        st.session_state[new_key] = st.session_state[old_key]


def restore_multiselect_filter(
    state_key, widget_key, options, *, allow_new_options=False
):
    saved_values = list(st.session_state.get(state_key, []))
    if not allow_new_options:
        saved_values = [value for value in saved_values if value in options]
    st.session_state[state_key] = saved_values
    restore_filter_widget(state_key, widget_key, saved_values)


def load_top_performer_symbols():
    df_top_perf = database.get_all_symbols_by_market_phase()
    top_perf_symbol_list = (
        df_top_perf["Symbol"].dropna().astype(str).str.upper().to_list()
    )
    if not top_perf_symbol_list:
        st.session_state["bt_results_top_performers_message"] = (
            "info",
            "No top performers found.",
        )
        return

    st.session_state["bt_results_saved_symbol"] = top_perf_symbol_list
    st.session_state["bt_results_apply_saved_symbol"] = True
    st.session_state["bt_results_top_performers_message"] = (
        "success",
        f"Loaded {len(top_perf_symbol_list)} top performer symbol(s).",
    )


migrate_filter_state("bt_results_strategy", "bt_results_saved_strategy")
migrate_filter_state("bt_results_timeframe", "bt_results_saved_timeframe")
migrate_filter_state("bt_results_symbol", "bt_results_saved_symbol")

df_bt_results = database.get_all_backtesting_results()

primary_filters = st.container(horizontal=True)
with primary_filters:
    strategy_options = sorted(
        dict_strategies.keys(),
        key=lambda strategy_id: (
            str(format_func_strategies(strategy_id)).casefold(),
            str(strategy_id).casefold(),
        ),
    )
    restore_multiselect_filter(
        "bt_results_saved_strategy",
        "_bt_results_strategy",
        strategy_options,
    )
    search_strategy = st.multiselect(
        "Strategy",
        options=strategy_options,
        format_func=format_func_strategies,
        key="_bt_results_strategy",
        on_change=lambda: persist_filter_widget(
            "bt_results_saved_strategy",
            "_bt_results_strategy",
        ),
    )

    list_timeframe = ["1w", "1d", "4h", "1h", "15m"]
    restore_multiselect_filter(
        "bt_results_saved_timeframe",
        "_bt_results_timeframe",
        list_timeframe,
    )
    search_timeframe = st.multiselect(
        label="Time-Frame",
        options=list_timeframe,
        key="_bt_results_timeframe",
        on_change=lambda: persist_filter_widget(
            "bt_results_saved_timeframe",
            "_bt_results_timeframe",
        ),
    )

# search by symbol
list_symbols = sorted(df_bt_results["Symbol"].dropna().astype(str).unique().tolist())
if "bt_results_saved_symbol" in st.session_state:
    list_symbols = sorted(
        set(list_symbols).union(st.session_state["bt_results_saved_symbol"])
    )
if st.session_state.pop("bt_results_apply_saved_symbol", False):
    st.session_state["_bt_results_symbol"] = st.session_state.get(
        "bt_results_saved_symbol",
        [],
    )

symbol_filters = st.container(horizontal=True, vertical_alignment="bottom")
with symbol_filters:
    restore_multiselect_filter(
        "bt_results_saved_symbol",
        "_bt_results_symbol",
        list_symbols,
        allow_new_options=True,
    )
    search_symbol = st.multiselect(
        label="Symbol",
        options=list_symbols,
        accept_new_options=True,
        key="_bt_results_symbol",
        on_change=lambda: persist_filter_widget(
            "bt_results_saved_symbol",
            "_bt_results_symbol",
        ),
    )

    if st.button(
        "Load Top Performers",
        key="bt_results_load_top_performers",
        icon=":material/add:",
    ):
        load_top_performer_symbols()
        st.rerun()

top_performers_message = st.session_state.pop("bt_results_top_performers_message", None)
if top_performers_message:
    message_type, message_text = top_performers_message
    if message_type == "success":
        st.success(message_text)
    else:
        st.info(message_text)

today = datetime.now()
four_years_ago = today.replace(year=today.year - 4)

with st.expander("Advanced filters", expanded=False):
    advanced_result_filters = st.container(horizontal=True, vertical_alignment="bottom")
    with advanced_result_filters:
        search_date_ini = st.date_input(
            label="Start date",
            value=four_years_ago,
            min_value=four_years_ago,
            max_value=today,
            format="DD.MM.YYYY",
            key="bt_results_start_date",
        )

        search_date_end = st.date_input(
            label="End date",
            value=today,
            min_value=search_date_ini,
            max_value=today,
            format="DD.MM.YYYY",
            key="bt_results_end_date",
        )

        quality_grade_options = ["A", "B", "C", "D", "F"]
        search_quality_grade = st.multiselect(
            "Quality Grade",
            options=quality_grade_options,
            key="bt_results_quality_grade",
        )

        search_trading_approved = st.selectbox(
            "Trading Approved",
            options=["All", "Approved", "Rejected"],
            key="bt_results_trading_approved",
        )

        search_return_pct = st.checkbox(
            "Return % > 0",
            value=False,
            key="bt_results_return_positive",
        )

df_bt_results["Backtest_Start_Date"] = pd.to_datetime(
    df_bt_results["Backtest_Start_Date"]
)
df_bt_results["Backtest_End_Date"] = pd.to_datetime(df_bt_results["Backtest_End_Date"])

if search_strategy:
    df_bt_results = df_bt_results[df_bt_results["Strategy_Id"].isin(search_strategy)]
if search_symbol:
    df_bt_results = df_bt_results[df_bt_results["Symbol"].isin(search_symbol)]
if search_timeframe:
    df_bt_results = df_bt_results[df_bt_results["Time_Frame"].isin(search_timeframe)]
if search_return_pct:
    df_bt_results = df_bt_results[df_bt_results["Return_Perc"] > 0]
if search_quality_grade and "Quality_Grade" in df_bt_results.columns:
    df_bt_results = df_bt_results[
        df_bt_results["Quality_Grade"].astype(str).str.upper().isin(search_quality_grade)
    ]
if search_trading_approved != "All" and "Trading_Approved" in df_bt_results.columns:
    approved_value = 1 if search_trading_approved == "Approved" else 0
    df_bt_results = df_bt_results[
        df_bt_results["Trading_Approved"].fillna(0).astype(int) == approved_value
    ]
if search_date_ini and search_date_end:
    start_date = datetime(
        search_date_ini.year, search_date_ini.month, search_date_ini.day
    )
    end_date = datetime(
        search_date_end.year, search_date_end.month, search_date_end.day
    )
    df_bt_results = df_bt_results[
        (df_bt_results["Backtest_Start_Date"] <= end_date)
        & (df_bt_results["Backtest_End_Date"] >= start_date)
    ]

df_bt_results = df_bt_results.copy()

df_bt_results["Backtest_CSV"] = df_bt_results.apply(
    lambda row: get_backtest_static_url(row, "csv"), axis=1
)

df_bt_results["Return_vs_BuyHold_Perc"] = (
    df_bt_results["Return_Perc"] - df_bt_results["BuyHold_Return_Perc"]
)
df_bt_results["Return_Drawdown_Ratio"] = (
    df_bt_results["Return_Perc"] / df_bt_results["Max_Drawdown_Perc"].abs()
).replace([float("inf"), -float("inf")], pd.NA)


def format_backtest_strategy_params(row):
    def _int_label(value):
        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        return str(int(numeric)) if numeric.is_integer() else f"{numeric:g}"

    def _pair_label(prefix, fast, slow):
        fast_label = _int_label(fast)
        slow_label = _int_label(slow)
        if fast_label is None or slow_label is None:
            return ""
        return f"{prefix} {fast_label}/{slow_label}"

    config = {}
    try:
        raw_config = row.get("Backtest_Config_JSON", "")
        config = json.loads(raw_config or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        config = {}

    strategy_params = (
        config.get("strategy_parameters", {})
        if isinstance(config, dict)
        else {}
    )
    parts = []

    definition_indicators = (
        strategy_params.get("definition_indicators", [])
        if isinstance(strategy_params, dict)
        else []
    )
    if isinstance(definition_indicators, list) and definition_indicators:
        grouped_indicators = {}
        for indicator in definition_indicators:
            if not isinstance(indicator, dict):
                continue
            name = str(indicator.get("name", "") or "").upper()
            if not name:
                continue
            period = _int_label(indicator.get("period"))
            if not period:
                continue
            grouped_indicators.setdefault(name, [])
            if period not in grouped_indicators[name]:
                grouped_indicators[name].append(period)
        for name, periods in grouped_indicators.items():
            if len(periods) == 1:
                parts.append(f"{name} {periods[0]}")
            elif len(periods) == 2:
                parts.append(f"{name} {periods[0]}/{periods[1]}")
            elif periods:
                parts.append(f"{name} {','.join(periods)}")
        if parts:
            return " | ".join(parts)

    moving_averages = (
        strategy_params.get("moving_averages", {})
        if isinstance(strategy_params, dict)
        else {}
    )
    if moving_averages:
        ema_label = _pair_label(
            "EMA",
            moving_averages.get("ema_fast"),
            moving_averages.get("ema_slow"),
        )
        if ema_label:
            parts.append(ema_label)

    hma_params = (
        strategy_params.get("hma_rsi_linreg", {})
        if isinstance(strategy_params, dict)
        else {}
    )
    if hma_params:
        hma_label = _pair_label(
            "HMA",
            hma_params.get("hma_fast"),
            hma_params.get("hma_slow"),
        )
        if hma_label:
            parts.append(hma_label)
        rsi_period = _int_label(hma_params.get("rsi_period"))
        if rsi_period:
            parts.append(f"RSI {rsi_period}")
        linreg_period = _int_label(hma_params.get("daily_linreg_period"))
        if linreg_period:
            parts.append(f"LINREG {linreg_period}")

    market_phase = (
        strategy_params.get("market_phase_filter", {})
        if isinstance(strategy_params, dict)
        else {}
    )
    if market_phase:
        phase_label = _pair_label(
            "Phase SMA",
            market_phase.get("sma_fast"),
            market_phase.get("sma_slow"),
        )
        if phase_label:
            parts.append(phase_label)

    if parts:
        return " | ".join(parts)

    fallback_label = _pair_label(
        "Fast/Slow",
        row.get("Ema_Fast"),
        row.get("Ema_Slow"),
    )
    return fallback_label or "n/a"


df_bt_results["Strategy_Params"] = df_bt_results.apply(
    format_backtest_strategy_params,
    axis=1,
)
if "Trading_Approved" in df_bt_results.columns:
    df_bt_results["Trading_Approved"] = (
        df_bt_results["Trading_Approved"].fillna(0).astype(int).astype(bool)
    )

results_columns_order = [
    "Symbol",
    "Strategy_Name",
    "Time_Frame",
    "Strategy_Params",
    "Quality_Score",
    "Quality_Grade",
    "Trading_Approved",
    "Trading_Rejection_Reasons",
    "Return_Perc",
    "BuyHold_Return_Perc",
    "Return_vs_BuyHold_Perc",
    "Max_Drawdown_Perc",
    "Return_Drawdown_Ratio",
    "Profit_Factor",
    "SQN",
    "Win_Rate_Perc",
    "Trades",
    "Best_Trade_Perc",
    "Worst_Trade_Perc",
    "Avg_Trade_Perc",
    "Expectancy_Perc",
    "Kelly_Criterion",
    "Max_Trade_Duration",
    "Avg_Trade_Duration",
    "Backtest_Start_Date",
    "Backtest_End_Date",
    "Backtest_CSV",
    "Strategy_Id",
]
df_bt_results_display = df_bt_results[
    [column for column in results_columns_order if column in df_bt_results.columns]
].copy()


def quality_score_cell_style(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return ""

    if pd.isna(value):
        return ""
    if value >= 85:
        return "background-color: #dcfce7; color: #166534; font-weight: 700;"
    if value >= 70:
        return "background-color: #ecfdf5; color: #047857; font-weight: 700;"
    if value >= 55:
        return "background-color: #fef9c3; color: #854d0e; font-weight: 700;"
    if value >= 40:
        return "background-color: #ffedd5; color: #9a3412; font-weight: 700;"
    return "background-color: #fee2e2; color: #991b1b; font-weight: 700;"


def quality_grade_cell_style(value):
    styles = {
        "A": "background-color: #dcfce7; color: #166534; font-weight: 800; text-align: center;",
        "B": "background-color: #ecfdf5; color: #047857; font-weight: 800; text-align: center;",
        "C": "background-color: #fef9c3; color: #854d0e; font-weight: 800; text-align: center;",
        "D": "background-color: #ffedd5; color: #9a3412; font-weight: 800; text-align: center;",
        "F": "background-color: #fee2e2; color: #991b1b; font-weight: 800; text-align: center;",
    }
    return styles.get(str(value).upper(), "")


styled_bt_results_display = df_bt_results_display.style
if "Quality_Score" in df_bt_results_display.columns:
    styled_bt_results_display = styled_bt_results_display.map(
        quality_score_cell_style,
        subset=["Quality_Score"],
    )
if "Quality_Grade" in df_bt_results_display.columns:
    styled_bt_results_display = styled_bt_results_display.map(
        quality_grade_cell_style,
        subset=["Quality_Grade"],
    )

grid_key_columns = [
    column
    for column in ["Strategy_Id", "Symbol", "Time_Frame"]
    if column in df_bt_results_display.columns
]
if grid_key_columns and not df_bt_results_display.empty:
    grid_signature = int(
        pd.util.hash_pandas_object(
            df_bt_results_display[grid_key_columns].astype(str),
            index=False,
        ).sum()
    )
else:
    grid_signature = 0

dataframe_event = st.dataframe(
    styled_bt_results_display,
    width="content",
    key=f"bt_results_grid_{len(df_bt_results_display)}_{grid_signature}",
    on_select="rerun",
    selection_mode="multi-row",
    column_config={
        "Strategy_Id": None,
        "Symbol": st.column_config.TextColumn("Symbol", pinned=True),
        "Strategy_Name": st.column_config.TextColumn("Strategy", pinned=True),
        "Time_Frame": st.column_config.TextColumn("TF"),
        "Strategy_Params": st.column_config.TextColumn("Params"),
        "Quality_Score": st.column_config.NumberColumn(
            "Quality", format="%.1f", width="small"
        ),
        "Quality_Grade": st.column_config.TextColumn("Grade", width="small"),
        "Trading_Approved": st.column_config.CheckboxColumn(
            "Approved", width="small"
        ),
        "Trading_Rejection_Reasons": st.column_config.TextColumn(
            "Rejection Reasons"
        ),
        "Return_Perc": st.column_config.NumberColumn("Return %", format="%.2f"),
        "BuyHold_Return_Perc": st.column_config.NumberColumn(
            "Buy & Hold %", format="%.2f"
        ),
        "Return_vs_BuyHold_Perc": st.column_config.NumberColumn(
            "Vs B&H %", format="%.2f"
        ),
        "Max_Drawdown_Perc": st.column_config.NumberColumn("Max DD %", format="%.2f"),
        "Return_Drawdown_Ratio": st.column_config.NumberColumn(
            "Return/DD", format="%.2f"
        ),
        "Profit_Factor": st.column_config.NumberColumn("Profit Factor", format="%.2f"),
        "SQN": st.column_config.NumberColumn("SQN", format="%.2f"),
        "Win_Rate_Perc": st.column_config.NumberColumn("Win Rate %", format="%.2f"),
        "Trades": st.column_config.NumberColumn("Trades", format="%d"),
        "Best_Trade_Perc": st.column_config.NumberColumn("Best %", format="%.2f"),
        "Worst_Trade_Perc": st.column_config.NumberColumn("Worst %", format="%.2f"),
        "Avg_Trade_Perc": st.column_config.NumberColumn("Avg Trade %", format="%.2f"),
        "Expectancy_Perc": st.column_config.NumberColumn("Expectancy %", format="%.2f"),
        "Kelly_Criterion": st.column_config.NumberColumn("Kelly", format="%.2f"),
        "Max_Trade_Duration": st.column_config.TextColumn("Max Duration"),
        "Avg_Trade_Duration": st.column_config.TextColumn("Avg Duration"),
        "Backtest_Start_Date": st.column_config.DatetimeColumn("Start"),
        "Backtest_End_Date": st.column_config.DatetimeColumn("End"),
        "Backtest_CSV": st.column_config.LinkColumn(
            "CSV",
            display_text="Open",
            help="Download/open the CSV report for this backtest.",
        ),
    },
)

row_count = len(df_bt_results_display)
row_label = "row" if row_count == 1 else "rows"
st.caption(f"{row_count} backtesting result {row_label}.")

selected_row = None
selected_html_path = None
selected_rows = dataframe_event.selection.rows
selected_target_rows = []
valid_selected_rows = [
    row_index
    for row_index in selected_rows
    if 0 <= row_index < len(df_bt_results_display)
]
if valid_selected_rows:
    selected_target_rows = [
        df_bt_results_display.iloc[row_index] for row_index in valid_selected_rows
    ]
    selected_row = selected_target_rows[0]
    selected_html_path = get_backtest_file_path(selected_row, "html")
else:
    pending_selection = st.session_state.get("bt_results_pending_selection")
    if pending_selection:
        pending_mask = (
            (df_bt_results_display["Strategy_Id"].astype(str) == str(pending_selection.get("strategy_id")))
            & (df_bt_results_display["Symbol"].astype(str) == str(pending_selection.get("symbol")))
            & (df_bt_results_display["Time_Frame"].astype(str) == str(pending_selection.get("timeframe")))
        )
        if pending_mask.any():
            selected_row = df_bt_results_display[pending_mask].iloc[0]
            selected_html_path = get_backtest_file_path(selected_row, "html")

if selected_target_rows:
    st.caption(f"{len(selected_target_rows)} result row(s) selected for backtesting.")

can_run_from_filters = (
    len(search_strategy) >= 1 and len(search_timeframe) >= 1 and len(search_symbol) >= 1
)
run_target_rows = selected_target_rows
if not run_target_rows and can_run_from_filters:
    run_target_rows = [
        build_selected_backtest_row(
            strategy_id=strategy_id,
            symbol=symbol,
            timeframe=timeframe,
        )
        for strategy_id in search_strategy
        for timeframe in search_timeframe
        for symbol in search_symbol
    ]

cont_buttons = st.container(horizontal=True)
if cont_buttons.button(
    "Refresh", key="refresh_results_grid", icon=":material/refresh:"
):
    st.rerun()

queue_message = st.session_state.pop("bt_results_queue_message", None)
if queue_message:
    message_type, message_text = queue_message
    if message_type == "success":
        st.success(message_text)
    else:
        st.info(message_text)

run_help = "Select one or more result rows, or choose one or more Strategies, Time-Frames and Symbols with filters."
if cont_buttons.button(
    "Run Selected Backtests",
    key="enqueue_selected_backtests",
    icon=":material/play_arrow:",
    disabled=not run_target_rows,
    help=run_help,
):
    optimization_warnings = get_optimization_combination_warnings(run_target_rows)
    if optimization_warnings:
        st.session_state["bt_results_pending_large_optimization_rows"] = [
            dict(row) for row in run_target_rows
        ]
        st.session_state["bt_results_pending_large_optimization_warnings"] = (
            optimization_warnings
        )
    else:
        queue_backtesting_rows_with_message(run_target_rows)
    st.rerun()

pending_large_optimization_rows = st.session_state.get(
    "bt_results_pending_large_optimization_rows"
)
pending_large_optimization_warnings = st.session_state.get(
    "bt_results_pending_large_optimization_warnings"
)
if pending_large_optimization_rows and pending_large_optimization_warnings:
    confirm_large_optimization_dialog(
        pending_large_optimization_rows,
        pending_large_optimization_warnings,
    )

if not run_target_rows:
    st.caption(
        "To run new backtests without existing results, select one or more Strategies, Time-Frames and Symbols."
    )

render_backtesting_jobs_status()

if selected_row is not None:
    st.subheader("Backtest Report")

    if os.path.exists(selected_html_path):
        with open(selected_html_path, "r", encoding="utf-8") as file:
            html_content = file.read()
        html_content = ensure_quality_score_in_report(html_content, selected_row)
        html_content = ensure_backtest_report_card_layout(html_content)
        html_content = ensure_trades_csv_download_in_report(html_content)
        render_open_html_link(selected_html_path, html_content=html_content)
        components.html(
            html_content,
            height=estimate_backtest_report_height(html_content),
            scrolling=False,
        )
    else:
        st.warning(
            format_missing_backtest_file_message(selected_row, selected_html_path, "html")
        )
else:
    st.caption("Select a row in the results grid to render the HTML report.")

if selected_row is not None:
    st.divider()
    render_ai_strategy_analysis_controls(selected_row)
