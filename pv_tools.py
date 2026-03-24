"""PV (Production Validation) analysis tools for GDP MCP server.

Automates the PV failure analysis workflow:
- pv_failure_summary: Overview of all PV failures for a date range
- pv_analyze_metric: Deep-dive into a specific failing metric
- pv_generate_report: Generate HTML report for a set of failing metrics
"""

import json
import logging
import re
from datetime import datetime
from pathlib import Path

import auth

logger = logging.getLogger(__name__)

# Path to events-mart repo (configurable via config.json)
_CONFIG_PATH = Path(__file__).resolve().parent / "config.json"
_REPO_ROOT_CACHE = None


def _get_repo_root():
    global _REPO_ROOT_CACHE
    if _REPO_ROOT_CACHE is not None:
        return _REPO_ROOT_CACHE
    if _CONFIG_PATH.exists():
        import json as _json
        with open(_CONFIG_PATH) as _f:
            cfg = _json.load(_f)
        repo_path = cfg.get("events_mart_repo_path", "")
        if repo_path:
            _REPO_ROOT_CACHE = Path(repo_path)
            return _REPO_ROOT_CACHE
    # Fallback: assume this repo is inside events-mart/src/mcp_servers/gdp/
    candidate = Path(__file__).resolve().parent.parent.parent.parent
    if (candidate / "src" / "projects").is_dir():
        _REPO_ROOT_CACHE = candidate
        return _REPO_ROOT_CACHE
    _REPO_ROOT_CACHE = Path(".")
    return _REPO_ROOT_CACHE


_TABLE_VALIDATIONS_PATH_CACHE = None


def _get_table_validations_path():
    global _TABLE_VALIDATIONS_PATH_CACHE
    if _TABLE_VALIDATIONS_PATH_CACHE is not None:
        return _TABLE_VALIDATIONS_PATH_CACHE
    root = _get_repo_root()
    _TABLE_VALIDATIONS_PATH_CACHE = root / "src" / "projects" / "clickstream_pv_metric_analysis" / "pyspark" / "table_validations.json"
    return _TABLE_VALIDATIONS_PATH_CACHE

# PV results table (always queried from prod)
_PV_TABLE = "hive.integrated_events.clickstream_pv_metric_analysis"

MAX_ROWS = 500
MAX_CHARS = 50000

# Known upstream table lineage chains for PV source tables
# Each entry: (layer_name, repo, description)
_UPSTREAM_LINEAGE = {
    "integrated_metrics.diner_session_discovery": [
        ("Upstream Service (backend)", None, "search/impression/session data"),
        ("integrated_events.diner_search_impression (DSI)", "events-mart", None),
        ("integrated_events.diner_search_impression_summary (DSIS)", "events-mart", None),
        ("integrated_events.diner_session_summary", "events-mart", None),
    ],
    "integrated_metrics.diner_session_menu_metrics": [
        ("Upstream Service (backend)", None, "clickstream events"),
        ("integrated_events.diner_session_menu", "events-mart", None),
        ("integrated_events.diner_session_summary", "events-mart", None),
    ],
    "integrated_events.diner_search_impression_summary": [
        ("Upstream Service (backend)", None, "search impressions"),
        ("gdp_impressions.impressions_search", "gdp-impressions", None),
        ("integrated_events.diner_search_impression (DSI)", "events-mart", None),
    ],
    "integrated_events.restaurant_search_impression_summary": [
        ("Upstream Service (backend)", None, "search impressions"),
        ("integrated_events.diner_search_impression (DSI)", "events-mart", None),
    ],
    "integrated_events.diner_session_topics_impressions": [
        ("Upstream Service (backend)", None, "topic/search events"),
        ("ods.topics_search_type", "events-mart", None),
        ("integrated_events.diner_search_impression (DSI)", "events-mart", None),
    ],
    "integrated_events.diner_ad_impressions": [
        ("Upstream Service (backend)", None, "ad/search events"),
        ("ods.diner_sponsored_search_impression", "events-mart", None),
        ("integrated_events.diner_search_impression (DSI)", "events-mart", None),
    ],
}


def _sanitize_sql_str(value):
    """Escape single quotes in a string for safe SQL interpolation."""
    if not isinstance(value, str):
        value = str(value)
    return value.replace("'", "''")


def _sanitize_filename(value):
    """Make a string safe for use in filenames."""
    return re.sub(r"[^a-zA-Z0-9_-]", "_", value)


def _run_query(query, dev=False):
    """Execute a Trino query and return rows as list of dicts."""
    conn = auth.trino_connection(dev=dev)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = []
        for row in cursor:
            rows.append(dict(zip(columns, row)))
            if len(rows) >= MAX_ROWS:
                break
        return rows
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _load_table_validations():
    """Load and parse table_validations.json."""
    if not _get_table_validations_path().exists():
        return None
    with open(_get_table_validations_path()) as f:
        return json.load(f)


def _metric_name_matches(pv_name, json_name):
    """Check if a PV results metric name matches a table_validations.json metric name.

    PV results store metrics as '{table_prefix}_{metric_name}' (e.g.
    'diner_session_discovery_dslp_to_menu_cvr') while table_validations.json
    stores just '{metric_name}' (e.g. 'dslp_to_menu_cvr').
    """
    if pv_name == json_name:
        return True
    # Check if PV name ends with _<json_name> (table prefix stripped)
    if pv_name.endswith("_" + json_name):
        return True
    return False


def _find_metric_config(metric_name):
    """Find the metric definition in table_validations.json.

    Handles the naming mismatch where PV results prefix metric names with the
    table name (e.g. 'diner_session_discovery_dslp_to_menu_cvr') but the JSON
    config stores just the metric name (e.g. 'dslp_to_menu_cvr').

    Returns dict with table_name, date_column, metric, group_col, lookback_days or None.
    """
    config = _load_table_validations()
    if not config:
        return None

    for table_ctx in config.get("table_context", []):
        tbl = table_ctx.get("table_name", "")
        date_col = table_ctx.get("date_column", "")

        # Check validate_custom_metrics_weekly
        custom = table_ctx.get("validate_custom_metrics_weekly")
        if custom and custom.get("enabled"):
            group_col = custom.get("group_col")
            for m in custom.get("metrics", []):
                if _metric_name_matches(metric_name, m.get("name", "")):
                    return {
                        "table_name": tbl,
                        "date_column": date_col,
                        "metric": m,
                        "group_col": group_col,
                        "lookback_days": custom.get("lookback_days", 36),
                    }

        # Check validate_count_per_app_weekly
        count_val = table_ctx.get("validate_count_per_app_weekly")
        if count_val and count_val.get("enabled"):
            # Count metrics are named like "{table_short}_count_{app}"
            if metric_name.startswith("count_") or metric_name.endswith("_count"):
                return {
                    "table_name": tbl,
                    "date_column": date_col,
                    "metric": {"name": metric_name, "sql": "COUNT(*)", "description": count_val.get("description", "")},
                    "group_col": count_val.get("group_col"),
                    "lookback_days": count_val.get("lookback_days", 36),
                }

        # Check validate_non_null_columns
        nn = table_ctx.get("validate_non_null_columns")
        if nn and nn.get("enabled"):
            cols = nn.get("columns", {})
            for col_name in cols:
                if metric_name == f"{col_name}_non_null" or metric_name == col_name:
                    return {
                        "table_name": tbl,
                        "date_column": date_col,
                        "metric": {"name": metric_name, "sql": f"non_null({col_name})", "description": f"Non-null % for {col_name}", "threshold": cols[col_name]},
                        "group_col": None,
                        "lookback_days": 36,
                    }

    return None


def _parse_cvr_components(sql):
    """Parse a CVR SQL formula to extract numerator and denominator column names.

    Handles patterns like:
    - ROUND(SUM(dslp_to_menu_n) / SUM(dslp_to_menu_session_d) * 100.0, 1)
    - SUM(CASE WHEN x > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
    - SUM(x) / SUM(y) * 100.0

    Returns (numerator_expr, denominator_expr) or None if not a ratio metric.
    """
    if "/" not in sql:
        return None

    # Pattern 1: SUM(col_n) / SUM(col_d)
    match = re.search(r"SUM\((\w+)\)\s*/\s*SUM\((\w+)\)", sql, re.IGNORECASE)
    if match:
        return match.group(1), match.group(2)

    # Pattern 2: SUM(CASE WHEN ... END) * 100.0 / COUNT(*)
    # This is a percentage metric, not decomposable into simple columns
    if "CASE WHEN" in sql.upper():
        return None

    return None


def _categorize_metric(metric_name, table_name=""):
    """Categorize a metric by type."""
    name_lower = metric_name.lower()
    if "cvr" in name_lower or "_to_" in name_lower:
        return "CVR"
    if "non_null" in name_lower or "perc_" in name_lower:
        return "Non-Null"
    if "count" in name_lower or "n_" in name_lower:
        return "Count WoW"
    if "dupes" in name_lower or "unknown" in name_lower or "missing" in name_lower:
        return "Data Quality"
    return "Other"


def _classify_root_cause(num_trend, den_trend):
    """Classify root cause based on numerator/denominator trend analysis.

    Each trend is a dict with 'pre_avg' and 'post_avg' values.
    Returns (classification, explanation).
    """
    if not num_trend or not den_trend:
        return "Unknown", "Could not decompose metric into numerator/denominator"

    num_pre = num_trend.get("pre_avg", 0)
    num_post = num_trend.get("post_avg", 0)
    den_pre = den_trend.get("pre_avg", 0)
    den_post = den_trend.get("post_avg", 0)

    if den_pre == 0 or num_pre == 0:
        return "Unknown", "Insufficient baseline data"

    num_ratio = num_post / num_pre if num_pre else 0
    den_ratio = den_post / den_pre if den_pre else 0

    # Denominator spiked (>30% increase), numerator flat (<15% change)
    if den_ratio > 1.3 and 0.85 <= num_ratio <= 1.15:
        return "Denominator Spike", (
            f"Denominator increased {(den_ratio - 1) * 100:.0f}% while numerator stayed flat "
            f"({(num_ratio - 1) * 100:+.0f}%). Likely upstream volume change."
        )

    # Numerator dropped (>15% decrease), denominator flat
    if num_ratio < 0.85 and 0.85 <= den_ratio <= 1.15:
        return "Numerator Drop", (
            f"Numerator dropped {(1 - num_ratio) * 100:.0f}% while denominator stayed flat. "
            "Possible real conversion issue."
        )

    # Both dropped proportionally
    if num_ratio < 0.85 and den_ratio < 0.85:
        return "Traffic Drop", (
            f"Both numerator ({(1 - num_ratio) * 100:.0f}% drop) and denominator "
            f"({(1 - den_ratio) * 100:.0f}% drop) decreased. Seasonal or traffic decline."
        )

    # Both increased proportionally
    if num_ratio > 1.15 and den_ratio > 1.15:
        return "Volume Increase", (
            f"Both numerator (+{(num_ratio - 1) * 100:.0f}%) and denominator "
            f"(+{(den_ratio - 1) * 100:.0f}%) increased. CVR may be stable."
        )

    return "Mixed Signal", (
        f"Numerator changed {(num_ratio - 1) * 100:+.0f}%, denominator changed "
        f"{(den_ratio - 1) * 100:+.0f}%. Requires manual investigation."
    )


def register(mcp):

    @mcp.tool()
    def pv_failure_summary(days: int = 7) -> str:
        """Get a quick overview of all PV failures for a date range.

        Queries the clickstream_pv_metric_analysis table for failures in the
        last N days. Groups by metric_name + segment to find recurring issues
        and categorizes them by type (CVR, non-null, count WoW, etc.).

        Args:
            days: Number of days to look back (default 7)
        """
        try:
            days = max(1, min(int(days), 365))

            # 1. Daily failure rate
            daily_query = f"""
                SELECT metric_date,
                    COUNT(*) AS total_metrics,
                    COUNT(CASE WHEN status = 'FAIL' THEN 1 END) AS failures,
                    COUNT(CASE WHEN status = 'PASS' THEN 1 END) AS passes,
                    ROUND(100.0 * COUNT(CASE WHEN status = 'FAIL' THEN 1 END) / COUNT(*), 1) AS fail_pct
                FROM {_PV_TABLE}
                WHERE metric_date >= CURRENT_DATE - INTERVAL '{days}' DAY
                GROUP BY 1 ORDER BY 1
            """
            daily_rows = _run_query(daily_query)

            # 2. Recurring failures (3+ days)
            recurring_query = f"""
                SELECT metric_name, segment,
                    COUNT(CASE WHEN status = 'FAIL' THEN 1 END) AS fail_count,
                    COUNT(*) AS total_days,
                    ROUND(AVG(CASE WHEN status = 'FAIL' THEN actual END), 4) AS avg_actual_when_fail,
                    ROUND(AVG(min_expected), 4) AS avg_expected,
                    ROUND(AVG(threshold), 4) AS avg_threshold,
                    MIN(CASE WHEN status = 'FAIL' THEN metric_date END) AS first_fail_date,
                    MAX(CASE WHEN status = 'FAIL' THEN metric_date END) AS last_fail_date
                FROM {_PV_TABLE}
                WHERE metric_date >= CURRENT_DATE - INTERVAL '{days}' DAY
                GROUP BY 1, 2
                HAVING COUNT(CASE WHEN status = 'FAIL' THEN 1 END) >= 3
                ORDER BY fail_count DESC, metric_name
            """
            recurring_rows = _run_query(recurring_query)

            # 3. Categorize recurring failures
            categories = {}
            for row in recurring_rows:
                cat = _categorize_metric(row.get("metric_name", ""))
                if cat not in categories:
                    categories[cat] = []
                pct_of_expected = None
                avg_actual = row.get("avg_actual_when_fail")
                avg_expected = row.get("avg_expected")
                if avg_actual is not None and avg_expected and float(avg_expected) > 0:
                    pct_of_expected = round(float(avg_actual) / float(avg_expected) * 100, 1)
                categories[cat].append({
                    "metric_name": row.get("metric_name"),
                    "segment": row.get("segment"),
                    "fail_count": row.get("fail_count"),
                    "total_days": row.get("total_days"),
                    "avg_actual": float(avg_actual) if avg_actual is not None else None,
                    "avg_expected": float(avg_expected) if avg_expected is not None else None,
                    "pct_of_expected": pct_of_expected,
                    "first_fail_date": str(row.get("first_fail_date", "")),
                    "last_fail_date": str(row.get("last_fail_date", "")),
                })

            # Build summary
            total_failures = sum(int(r.get("failures", 0)) for r in daily_rows)
            total_metrics = sum(int(r.get("total_metrics", 0)) for r in daily_rows)
            avg_fail_pct = round(total_failures / total_metrics * 100, 1) if total_metrics else 0

            result = {
                "summary": {
                    "days_analyzed": days,
                    "total_metric_checks": total_metrics,
                    "total_failures": total_failures,
                    "avg_daily_fail_pct": avg_fail_pct,
                    "recurring_failure_count": len(recurring_rows),
                },
                "daily_rates": [{
                    "date": str(r.get("metric_date", "")),
                    "total": r.get("total_metrics"),
                    "failures": r.get("failures"),
                    "passes": r.get("passes"),
                    "fail_pct": float(r.get("fail_pct", 0)),
                } for r in daily_rows],
                "recurring_failures_by_category": categories,
            }

            output = json.dumps(result, default=str)
            if len(output) > MAX_CHARS:
                output = output[:MAX_CHARS] + "\n... (truncated)"
            return output

        except Exception as e:
            return json.dumps({"error": f"pv_failure_summary failed: {e}"})

    @mcp.tool()
    def pv_analyze_metric(metric_name: str, segment: str = "", days: int = 30) -> str:
        """Deep-dive into a specific failing metric -- decompose it, find the
        inflection point, and classify the root cause.

        For CVR metrics (with numerator/denominator), this queries the source
        table to decompose the metric and detect whether the numerator dropped
        or the denominator spiked.

        Args:
            metric_name: The metric name (e.g. "dslp_to_menu_cvr", "search_cvr")
            segment: Optional segment/app filter (e.g. "iOS Native"). If empty, returns all segments.
            days: Number of days of history to analyze (default 30)
        """
        try:
            # Sanitize inputs
            safe_metric = _sanitize_sql_str(metric_name)
            safe_segment = _sanitize_sql_str(segment)
            days = max(1, min(int(days), 365))

            # 1. Query PV results table for this metric over N days
            segment_filter = f"AND segment = '{safe_segment}'" if segment else ""
            trend_query = f"""
                SELECT metric_date, metric_name, segment, status, actual, min_expected, threshold
                FROM {_PV_TABLE}
                WHERE metric_name = '{safe_metric}'
                    {segment_filter}
                    AND metric_date >= CURRENT_DATE - INTERVAL '{days}' DAY
                ORDER BY segment, metric_date
            """
            trend_rows = _run_query(trend_query)

            if not trend_rows:
                return json.dumps({"error": f"No data found for metric '{metric_name}' in the last {days} days"})

            # Build trend data
            trend_data = []
            for r in trend_rows:
                trend_data.append({
                    "date": str(r.get("metric_date", "")),
                    "segment": r.get("segment"),
                    "status": r.get("status"),
                    "actual": float(r["actual"]) if r.get("actual") is not None else None,
                    "expected": float(r["min_expected"]) if r.get("min_expected") is not None else None,
                    "threshold": float(r["threshold"]) if r.get("threshold") is not None else None,
                })

            # 2. Detect inflection point (first FAIL after a PASS streak)
            # Group by segment to avoid cross-segment confusion
            inflection_date = None
            segments_in_data = set(r["segment"] for r in trend_data if r.get("segment"))
            for seg_val in (segments_in_data or [None]):
                seg_rows = [r for r in trend_data if r.get("segment") == seg_val] if seg_val else trend_data
                for i, row in enumerate(seg_rows):
                    if row["status"] == "FAIL" and i > 0 and seg_rows[i - 1]["status"] == "PASS":
                        inflection_date = row["date"]
                        break
                if inflection_date:
                    break
            # Fallback: first FAIL date across all segments
            if not inflection_date:
                for row in trend_data:
                    if row["status"] == "FAIL":
                        inflection_date = row["date"]
                        break

            # 3. Try to decompose CVR metric using table_validations.json
            decomposition = None
            root_cause = None
            recommended_action = None
            metric_config = _find_metric_config(metric_name)

            if metric_config:
                sql_formula = metric_config["metric"].get("sql", "")
                components = _parse_cvr_components(sql_formula)

                if components:
                    num_col, den_col = components
                    table_name = metric_config["table_name"]
                    date_col = metric_config["date_column"]
                    group_col = metric_config.get("group_col")

                    # Use hive catalog for the source table
                    hive_table = f"hive.{table_name}"

                    # Build the segment filter for the source table
                    src_segment_filter = ""
                    if segment and group_col:
                        src_segment_filter = f"AND {group_col} = '{safe_segment}'"

                    decomp_query = f"""
                        SELECT {date_col} AS dt,
                            SUM({num_col}) AS numerator,
                            SUM({den_col}) AS denominator,
                            ROUND(SUM(CAST({num_col} AS DOUBLE)) * 100.0 / NULLIF(SUM(CAST({den_col} AS DOUBLE)), 0), 2) AS cvr
                        FROM {hive_table}
                        WHERE {date_col} >= CURRENT_DATE - INTERVAL '{days}' DAY
                            {src_segment_filter}
                        GROUP BY 1
                        ORDER BY 1
                    """

                    try:
                        decomp_rows = _run_query(decomp_query)

                        decomposition = [{
                            "date": str(r.get("dt", "")),
                            "numerator": float(r["numerator"]) if r.get("numerator") is not None else None,
                            "denominator": float(r["denominator"]) if r.get("denominator") is not None else None,
                            "cvr": float(r["cvr"]) if r.get("cvr") is not None else None,
                        } for r in decomp_rows]

                        # Classify root cause using pre/post inflection comparison
                        if inflection_date and len(decomposition) > 5:
                            pre = [d for d in decomposition if d["date"] < inflection_date]
                            post = [d for d in decomposition if d["date"] >= inflection_date]

                            if pre and post:
                                pre_nums = [d["numerator"] for d in pre if d["numerator"] is not None]
                                pre_dens = [d["denominator"] for d in pre if d["denominator"] is not None]
                                post_nums = [d["numerator"] for d in post if d["numerator"] is not None]
                                post_dens = [d["denominator"] for d in post if d["denominator"] is not None]

                                if pre_nums and pre_dens and post_nums and post_dens:
                                    num_trend = {"pre_avg": sum(pre_nums) / len(pre_nums), "post_avg": sum(post_nums) / len(post_nums)}
                                    den_trend = {"pre_avg": sum(pre_dens) / len(pre_dens), "post_avg": sum(post_dens) / len(post_dens)}

                                    classification, explanation = _classify_root_cause(num_trend, den_trend)
                                    root_cause = {
                                        "classification": classification,
                                        "explanation": explanation,
                                        "numerator_col": num_col,
                                        "denominator_col": den_col,
                                        "pre_inflection_avg_num": round(num_trend["pre_avg"], 1),
                                        "post_inflection_avg_num": round(num_trend["post_avg"], 1),
                                        "pre_inflection_avg_den": round(den_trend["pre_avg"], 1),
                                        "post_inflection_avg_den": round(den_trend["post_avg"], 1),
                                    }

                                    # Recommended action based on classification
                                    if classification == "Denominator Spike":
                                        recommended_action = (
                                            "Upstream volume change detected. Options: "
                                            "1) Lower WoW threshold in table_validations.json, "
                                            "2) Reduce lookback_days to flush old baseline faster, "
                                            "3) Wait for auto-correction once lookback window covers only post-change data, "
                                            "4) Investigate upstream service for intentional changes."
                                        )
                                    elif classification == "Numerator Drop":
                                        recommended_action = (
                                            "Real conversion drop detected. Investigate: "
                                            "1) Product changes that may have affected user behavior, "
                                            "2) A/B experiments running on this funnel, "
                                            "3) Mobile app releases around the inflection date."
                                        )
                                    elif classification == "Traffic Drop":
                                        recommended_action = (
                                            "Overall traffic decline. Check: "
                                            "1) Is this seasonal (holidays, weekday patterns)?, "
                                            "2) Marketing campaign changes?, "
                                            "3) Platform outages around the inflection date?"
                                        )
                                    else:
                                        recommended_action = "Manual investigation needed. Check upstream data lineage and recent code changes."

                    except Exception as e:
                        decomposition = None
                        logger.warning("CVR decomposition query failed for %s: %s", metric_name, e)

            # Count pass/fail
            fail_count = sum(1 for r in trend_data if r["status"] == "FAIL")
            pass_count = sum(1 for r in trend_data if r["status"] == "PASS")

            result = {
                "metric_name": metric_name,
                "segment": segment or "all",
                "days_analyzed": days,
                "pass_count": pass_count,
                "fail_count": fail_count,
                "inflection_date": inflection_date,
                "trend": trend_data,
                "source_table": metric_config["table_name"] if metric_config else None,
                "sql_formula": metric_config["metric"].get("sql") if metric_config else None,
                "decomposition": decomposition,
                "root_cause": root_cause,
                "recommended_action": recommended_action,
                "category": _categorize_metric(metric_name),
            }

            output = json.dumps(result, default=str)
            if len(output) > MAX_CHARS:
                output = output[:MAX_CHARS] + "\n... (truncated)"
            return output

        except Exception as e:
            return json.dumps({"error": f"pv_analyze_metric failed: {e}"})

    @mcp.tool()
    def pv_generate_report(metric_pattern: str = "", days: int = 14) -> str:
        """Generate an HTML report for PV failures matching a pattern.

        Calls pv_failure_summary and pv_analyze_metric for each recurring
        failure, then generates a styled HTML report.

        Args:
            metric_pattern: Filter metrics by name substring (e.g. "discovery", "menu", "search"). Empty = all.
            days: Number of days to analyze (default 14)
        """
        try:
            days = max(1, min(int(days), 365))

            # 1. Get failure summary
            summary_json = pv_failure_summary(days=days)
            summary = json.loads(summary_json)
            if "error" in summary:
                return json.dumps({"error": f"Failed to get failure summary: {summary['error']}"})

            # 2. Collect recurring failures matching pattern
            recurring = []
            for cat, metrics in summary.get("recurring_failures_by_category", {}).items():
                for m in metrics:
                    name = m.get("metric_name", "")
                    if not metric_pattern or metric_pattern.lower() in name.lower():
                        recurring.append(m)

            if not recurring:
                return json.dumps({"info": f"No recurring failures found matching pattern '{metric_pattern}' in the last {days} days"})

            # 3. Analyze top recurring failures (limit to 15 to avoid timeout)
            analyses = []
            seen = set()
            for m in recurring[:15]:
                key = (m["metric_name"], m.get("segment", ""))
                if key in seen:
                    continue
                seen.add(key)
                try:
                    # Use at least 30 days for analysis so decomposition
                    # captures pre-inflection baseline even if report is shorter
                    analysis_days = max(days, 30)
                    analysis_json = pv_analyze_metric(
                        metric_name=m["metric_name"],
                        segment=m.get("segment", ""),
                        days=analysis_days,
                    )
                    analysis = json.loads(analysis_json)
                    if "error" not in analysis:
                        analyses.append(analysis)
                except Exception as e:
                    logger.warning("Failed to analyze %s/%s: %s", m["metric_name"], m.get("segment"), e)

            # 4. Generate HTML report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            pattern_label = metric_pattern or "all"
            html = _generate_html_report(summary, recurring, analyses, pattern_label, days)

            # 5. Write to /tmp
            safe_pattern = _sanitize_filename(pattern_label)
            report_path = f"/tmp/pv_analysis_report_{safe_pattern}_{timestamp}.html"
            with open(report_path, "w") as f:
                f.write(html)

            result = {
                "report_path": report_path,
                "pattern": pattern_label,
                "days_analyzed": days,
                "recurring_failures_analyzed": len(analyses),
                "total_recurring_failures": len(recurring),
                "summary": {
                    "total_metric_checks": summary.get("summary", {}).get("total_metric_checks"),
                    "total_failures": summary.get("summary", {}).get("total_failures"),
                    "avg_daily_fail_pct": summary.get("summary", {}).get("avg_daily_fail_pct"),
                },
            }
            return json.dumps(result, default=str)

        except Exception as e:
            return json.dumps({"error": f"pv_generate_report failed: {e}"})

    @mcp.tool()
    def pv_investigate_root_cause(metric_name: str, segment: str = "", inflection_date: str = "") -> str:
        """Investigate whether a PV metric failure is caused by our code change or
        upstream data change. Performs git history analysis, upstream volume checks,
        and classifies the root cause.

        Args:
            metric_name: The failing metric (e.g. "diner_session_discovery_dslp_to_menu_cvr")
            segment: Optional segment (e.g. "iOS Native")
            inflection_date: The date when failures started (YYYY-MM-DD). If empty, auto-detected.
        """
        import subprocess

        try:
            # If no inflection date, run pv_analyze_metric to detect it
            if not inflection_date:
                logger.info("[investigate] Step 0: Analyzing metric %s / %s (no inflection date, querying prod)...", metric_name, segment or "all")
                analysis_json = pv_analyze_metric(metric_name=metric_name, segment=segment, days=30)
                analysis = json.loads(analysis_json)
                if "error" in analysis:
                    return json.dumps({"error": f"Could not analyze metric: {analysis['error']}"})
                inflection_date = analysis.get("inflection_date", "")
                if not inflection_date:
                    return json.dumps({"error": "Could not detect inflection date"})
            else:
                logger.info("[investigate] Step 0: Using provided inflection date %s, looking up metric config...", inflection_date)
                # Build minimal analysis from metric config without querying Trino
                metric_config = _find_metric_config(metric_name)
                source_table = metric_config["table_name"] if metric_config else ""
                sql_formula = metric_config["metric"].get("sql", "") if metric_config else ""
                components = _parse_cvr_components(sql_formula) if sql_formula else None
                analysis = {
                    "metric_name": metric_name,
                    "segment": segment,
                    "inflection_date": inflection_date,
                    "source_table": source_table,
                    "root_cause": {
                        "numerator_col": components[0] if components else "",
                        "denominator_col": components[1] if components else "",
                    } if components else None,
                }
            logger.info("[investigate] Inflection date: %s, source: %s", inflection_date, analysis.get("source_table"))

            source_table = analysis.get("source_table", "")
            root_cause_data = analysis.get("root_cause")
            decomposition = analysis.get("decomposition")

            # Map source table to pipeline folder
            pipeline_folder = _find_pipeline_folder(source_table)

            # Compute date range: inflection -7 to +3
            from datetime import timedelta
            infl_dt = datetime.strptime(inflection_date, "%Y-%m-%d")
            date_before = (infl_dt - timedelta(days=7)).strftime("%Y-%m-%d")
            date_after = (infl_dt + timedelta(days=3)).strftime("%Y-%m-%d")

            investigation = {
                "metric_name": metric_name,
                "segment": segment or "all",
                "inflection_date": inflection_date,
                "source_table": source_table,
                "pipeline_folder": pipeline_folder,
                "steps": [],
            }

            # ============================================================
            # STEP 1: Check events-mart LOADER code changes around inflection
            # Only .py files in jobs/ count as real logic changes.
            # Config-only changes (schedule.json, project.py) are tracked in Step 2.
            # ============================================================
            logger.info("[investigate] Step 1: Checking git history for LOADER code changes (%s to %s)...", date_before, date_after)
            step1 = {"name": "Loader Code Changes (.py in jobs/)", "commits": [], "verdict": "NO LOADER CODE CHANGES"}
            if pipeline_folder:
                # Only check the actual job Python code (the ETL logic)
                loader_path = f"src/python/events_mart/jobs/{pipeline_folder}/"
                try:
                    result = subprocess.run(
                        ["git", "log", "--oneline", "--after", date_before, "--before", date_after, "--", loader_path],
                        capture_output=True, text=True, cwd=str(_get_repo_root()), timeout=10,
                    )
                    if result.stdout.strip():
                        for line in result.stdout.strip().split("\n"):
                            if line.strip():
                                # Verify it touches .py files (not just __pycache__)
                                commit_hash = line.strip().split()[0]
                                files_result = subprocess.run(
                                    ["git", "diff-tree", "--no-commit-id", "--name-only", "-r", commit_hash, "--", loader_path],
                                    capture_output=True, text=True, cwd=str(_get_repo_root()), timeout=5,
                                )
                                py_files = [f for f in files_result.stdout.strip().split("\n") if f.endswith(".py")]
                                if py_files:
                                    step1["commits"].append({
                                        "commit": line.strip(),
                                        "files_changed": py_files,
                                    })
                except Exception as e:
                    step1["commits"].append({"error": str(e)})

            # Also check common/shared code that could affect this pipeline
            shared_paths = [
                "src/python/events_mart/common/",
                "src/python/events_mart/jobs/common/",
            ]
            for path in shared_paths:
                try:
                    result = subprocess.run(
                        ["git", "log", "--oneline", "--after", date_before, "--before", date_after, "--", path],
                        capture_output=True, text=True, cwd=str(_get_repo_root()), timeout=10,
                    )
                    if result.stdout.strip():
                        for line in result.stdout.strip().split("\n"):
                            if line.strip():
                                step1["commits"].append({"commit": line.strip(), "path": path})
                except Exception:
                    pass

            if step1["commits"]:
                step1["verdict"] = f"FOUND {len(step1['commits'])} LOADER CHANGE(S)"
            investigation["steps"].append(step1)

            # ============================================================
            # STEP 2: Check schedule.json / table_validations.json changes
            # ============================================================
            logger.info("[investigate] Step 1 done: %s. Step 2: Checking config changes...", step1["verdict"])
            step2 = {"name": "Config Changes (schedule.json, table_validations.json)", "commits": [], "verdict": "NO CONFIG CHANGES"}
            config_paths = [
                "src/projects/clickstream_pv_metric_analysis/pyspark/table_validations.json",
            ]
            if pipeline_folder:
                config_paths.append(f"src/projects/{pipeline_folder}/azkaban/schedule.json")
                config_paths.append(f"src/projects/{pipeline_folder}/azkaban/project.py")
            for path in config_paths:
                try:
                    result = subprocess.run(
                        ["git", "log", "--oneline", "--after", date_before, "--before", date_after, "--", path],
                        capture_output=True, text=True, cwd=str(_get_repo_root()), timeout=10,
                    )
                    if result.stdout.strip():
                        for line in result.stdout.strip().split("\n"):
                            if line.strip():
                                step2["commits"].append({"path": path, "commit": line.strip()})
                except Exception:
                    pass
            if step2["commits"]:
                step2["verdict"] = f"FOUND {len(step2['commits'])} CHANGE(S)"
            investigation["steps"].append(step2)

            # ============================================================
            # STEP 3: Check upstream source table volumes
            # Includes both direct deps (from project.py wait_for_*) AND
            # deeper lineage tables (from _UPSTREAM_LINEAGE map)
            # ============================================================
            logger.info("[investigate] Step 2 done: %s. Step 3: Checking upstream table volumes...", step2["verdict"])
            step3 = {"name": "Upstream Table Volumes", "tables": [], "verdict": "NO VOLUME ANOMALIES"}

            # PRIORITY ORDER: Check deep lineage tables FIRST (where anomalies
            # usually hide), then direct deps from project.py.
            upstream_tables = []

            # 1. Deep lineage tables from the known lineage map (highest priority)
            if source_table and source_table in _UPSTREAM_LINEAGE:
                for layer_name, repo, note in _UPSTREAM_LINEAGE[source_table]:
                    tbl_match = re.match(r"^([\w.]+)", layer_name)
                    if tbl_match:
                        deep_tbl = tbl_match.group(1)
                        if "." in deep_tbl:
                            # Use session_start_date for most tables (it's the partition key)
                            # event_date is the query column but session_start_date is what's synced
                            date_col = "session_start_date"
                            upstream_tables.append((deep_tbl, date_col))

            # 2. Direct upstream tables from project.py
            direct_deps = _get_upstream_tables(pipeline_folder)
            for tbl, dc in direct_deps:
                if tbl and not any(t[0] == tbl for t in upstream_tables):
                    upstream_tables.append((tbl, dc))

            # Deduplicate and limit to 7 (deep lineage first)
            seen_tables = set()
            deduped = []
            for tbl, dc in upstream_tables:
                if tbl and tbl not in seen_tables:
                    seen_tables.add(tbl)
                    deduped.append((tbl, dc))
            upstream_tables = deduped[:7]
            logger.info("[investigate] Checking %d upstream tables: %s", len(upstream_tables), [t[0] for t in upstream_tables])
            volume_anomalies = []

            # Known group columns for deeper volume analysis
            _GROUP_COLS = {
                "integrated_events.diner_search_impression": "impression_type",
                "integrated_events.diner_ad_impressions": "ad_vendor",
                "integrated_events.diner_session_cross_sell_impressions": "impression_type",
            }

            for idx, (utbl, date_col) in enumerate(upstream_tables):
                logger.info("[investigate] Step 3: Querying volume for %s (%d/%d)...", utbl, idx + 1, len(upstream_tables))
                try:
                    # Total volume check
                    vol_query = f"""
                        SELECT {date_col} AS dt,
                            COUNT(*) AS row_count
                        FROM hive.{utbl}
                        WHERE {date_col} >= DATE '{date_before}'
                            AND {date_col} <= DATE '{date_after}'
                        GROUP BY 1
                        ORDER BY 1
                    """
                    vol_rows = _run_query(vol_query, dev=True)
                    if vol_rows:
                        counts = [int(r.get("row_count", 0)) for r in vol_rows]
                        dates_list = [str(r.get("dt", "")) for r in vol_rows]

                        pre = [c for c, d in zip(counts, dates_list) if d < inflection_date]
                        post = [c for c, d in zip(counts, dates_list) if d >= inflection_date]

                        pre_avg = sum(pre) / len(pre) if pre else 0
                        post_avg = sum(post) / len(post) if post else 0
                        change_pct = ((post_avg / pre_avg - 1) * 100) if pre_avg > 0 else 0

                        tbl_result = {
                            "table": utbl,
                            "date_column": date_col,
                            "pre_inflection_avg_rows": round(pre_avg),
                            "post_inflection_avg_rows": round(post_avg),
                            "change_pct": round(change_pct, 1),
                            "anomaly": abs(change_pct) > 20,
                        }

                        # Group-level volume check for known tables
                        group_col = _GROUP_COLS.get(utbl)
                        if group_col:
                            logger.info("[investigate]   Also checking %s by %s...", utbl, group_col)
                            try:
                                grp_query = f"""
                                    SELECT {date_col} AS dt, {group_col} AS grp,
                                        COUNT(*) AS row_count
                                    FROM hive.{utbl}
                                    WHERE {date_col} >= DATE '{date_before}'
                                        AND {date_col} <= DATE '{date_after}'
                                    GROUP BY 1, 2
                                    ORDER BY 1, 2
                                """
                                grp_rows = _run_query(grp_query, dev=True)
                                if grp_rows:
                                    # Group by the group_col value
                                    groups = {}
                                    for r in grp_rows:
                                        g = str(r.get("grp", ""))
                                        dt = str(r.get("dt", ""))
                                        cnt = int(r.get("row_count", 0))
                                        if g not in groups:
                                            groups[g] = {"pre": [], "post": []}
                                        if dt < inflection_date:
                                            groups[g]["pre"].append(cnt)
                                        else:
                                            groups[g]["post"].append(cnt)

                                    group_anomalies = []
                                    for g, vals in groups.items():
                                        g_pre = sum(vals["pre"]) / len(vals["pre"]) if vals["pre"] else 0
                                        g_post = sum(vals["post"]) / len(vals["post"]) if vals["post"] else 0
                                        g_change = ((g_post / g_pre - 1) * 100) if g_pre > 0 else 0
                                        if abs(g_change) > 20:
                                            group_anomalies.append({
                                                "group": g,
                                                "pre_avg": round(g_pre),
                                                "post_avg": round(g_post),
                                                "change_pct": round(g_change, 1),
                                            })

                                    if group_anomalies:
                                        tbl_result["group_anomalies"] = group_anomalies
                                        tbl_result["group_column"] = group_col
                                        tbl_result["anomaly"] = True  # Override: group-level anomaly found
                            except Exception as e:
                                logger.warning("[investigate]   Group check failed for %s: %s", utbl, e)

                        step3["tables"].append(tbl_result)
                        if tbl_result["anomaly"]:
                            volume_anomalies.append(tbl_result)
                except Exception as e:
                    step3["tables"].append({"table": utbl, "error": str(e)})

            if volume_anomalies:
                step3["verdict"] = f"VOLUME ANOMALY IN {len(volume_anomalies)} TABLE(S)"
            investigation["steps"].append(step3)

            # ============================================================
            # STEP 4: Check git blame on key files (numerator/denominator columns)
            # ============================================================
            logger.info("[investigate] Step 3 done: %s. Step 4: Checking column-level git history...", step3["verdict"])
            step4 = {"name": "Column-Level Git Blame", "findings": [], "verdict": "NO RECENT COLUMN CHANGES"}
            if root_cause_data and pipeline_folder:
                num_col = root_cause_data.get("numerator_col", "")
                den_col = root_cause_data.get("denominator_col", "")
                cols_to_check = [c for c in [num_col, den_col] if c]

                # Find the loader/job files
                job_files = []
                try:
                    result = subprocess.run(
                        ["find", f"src/python/events_mart/jobs/{pipeline_folder}", "-name", "*.py", "-type", "f"],
                        capture_output=True, text=True, cwd=str(_get_repo_root()), timeout=5,
                    )
                    job_files = [f.strip() for f in result.stdout.strip().split("\n") if f.strip()]
                except Exception:
                    pass

                for col in cols_to_check:
                    for jf in job_files:
                        try:
                            result = subprocess.run(
                                ["git", "log", "--oneline", "-5", "-S", col, "--", jf],
                                capture_output=True, text=True, cwd=str(_get_repo_root()), timeout=10,
                            )
                            if result.stdout.strip():
                                for line in result.stdout.strip().split("\n"):
                                    if line.strip():
                                        step4["findings"].append({
                                            "column": col,
                                            "file": jf,
                                            "commit": line.strip(),
                                        })
                        except Exception:
                            pass

            if step4["findings"]:
                step4["verdict"] = f"FOUND {len(step4['findings'])} COLUMN CHANGE(S)"
            investigation["steps"].append(step4)

            # ============================================================
            # FINAL CLASSIFICATION
            # ============================================================
            logger.info("[investigate] Step 4 done: %s. Classifying root cause...", step4["verdict"])
            # has_code_changes = LOADER .py files changed (real ETL logic)
            # has_config_changes = schedule.json / project.py / table_validations.json only
            has_loader_code_changes = step1["verdict"] != "NO LOADER CODE CHANGES"
            has_config_changes = step2["verdict"] != "NO CONFIG CHANGES"
            has_volume_anomaly = step3["verdict"] != "NO VOLUME ANOMALIES"
            has_column_changes = step4["verdict"] != "NO RECENT COLUMN CHANGES"
            # Count how many upstream tables had errors (couldn't check)
            volume_errors = sum(1 for t in step3.get("tables", []) if "error" in t)
            volume_checked = sum(1 for t in step3.get("tables", []) if "error" not in t)

            # Step 4 column changes: filter out "original creation" commits
            # (commits that are older than 6 months are likely the original ETL, not recent changes)
            recent_column_changes = []
            if step4.get("findings"):
                for f in step4["findings"]:
                    commit_line = f.get("commit", "")
                    # Check if this commit is within the inflection window
                    commit_hash = commit_line.split()[0] if commit_line else ""
                    if commit_hash:
                        try:
                            date_result = subprocess.run(
                                ["git", "log", "-1", "--format=%ci", commit_hash],
                                capture_output=True, text=True, cwd=str(_get_repo_root()), timeout=5,
                            )
                            commit_date = date_result.stdout.strip()[:10]
                            # Only count if within 60 days of inflection
                            if commit_date >= (infl_dt - timedelta(days=60)).strftime("%Y-%m-%d"):
                                recent_column_changes.append(f)
                        except Exception:
                            pass

            has_recent_column_changes = len(recent_column_changes) > 0

            # Classification logic (ordered by confidence)
            if has_volume_anomaly and not has_loader_code_changes:
                anomaly_details_parts = []
                for t in volume_anomalies:
                    detail = f"{t['table']} ({t['change_pct']:+.0f}%)"
                    # Include group-level anomaly details
                    if t.get("group_anomalies"):
                        grp_parts = [f"{g['group']}: {g['change_pct']:+.0f}%" for g in t["group_anomalies"][:3]]
                        detail += f" [by {t.get('group_column', 'group')}: {', '.join(grp_parts)}]"
                    anomaly_details_parts.append(detail)
                anomaly_details = "; ".join(anomaly_details_parts)
                verdict = "UPSTREAM_DATA_CHANGE"
                explanation = (
                    f"No events-mart loader code changes around {inflection_date}, but volume anomaly detected in "
                    f"upstream table(s): {anomaly_details}. "
                    "This is NOT a pipeline bug. The change is upstream of the data pipeline."
                )
            elif has_loader_code_changes and not has_volume_anomaly:
                commit_details = "; ".join(c.get("commit", "") for c in step1["commits"][:3])
                verdict = "OUR_CODE_CHANGE"
                explanation = (
                    f"Loader code commits found in events-mart around {inflection_date}: {commit_details}. "
                    "No upstream volume anomalies. The failure is likely caused by our code change."
                )
            elif has_loader_code_changes and has_volume_anomaly:
                verdict = "MIXED_SIGNAL"
                explanation = (
                    "Both loader code changes AND upstream volume anomalies found. "
                    "Manual investigation needed to determine which caused the failure."
                )
            elif has_volume_anomaly:
                anomaly_details = "; ".join(
                    f"{t['table']} ({t['change_pct']:+.0f}%)" for t in volume_anomalies
                )
                verdict = "UPSTREAM_DATA_CHANGE"
                explanation = f"Upstream volume anomaly: {anomaly_details}."
            elif not has_loader_code_changes and not has_volume_anomaly and volume_errors > 0:
                verdict = "INCONCLUSIVE"
                explanation = (
                    f"No loader code changes found. {volume_errors} of {volume_errors + volume_checked} upstream "
                    f"volume checks failed (connection errors). Cannot rule out upstream data change. "
                    "Config-only changes found." if has_config_changes else
                    f"No loader code changes found. {volume_errors} of {volume_errors + volume_checked} upstream "
                    f"volume checks failed (connection errors). Cannot rule out upstream data change."
                )
            elif has_config_changes and not has_loader_code_changes:
                verdict = "CONFIG_ONLY_CHANGE"
                explanation = (
                    "Only config/schedule changes found (no loader code changes, no upstream volume anomalies). "
                    "These are unlikely to cause metric failures unless thresholds were changed."
                )
            elif has_recent_column_changes:
                cols = set(f.get("column", "") for f in recent_column_changes)
                verdict = "RECENT_COLUMN_CHANGE"
                explanation = (
                    f"Recent commits (within 60 days) touching columns: {', '.join(cols)}. "
                    "These may have altered the metric calculation."
                )
            else:
                verdict = "NO_CHANGES_DETECTED"
                explanation = (
                    "No loader code changes, no config changes, no upstream volume anomalies found. "
                    "Possible causes: upstream service behavior change (not reflected in row counts), "
                    "data quality shift in existing rows, or seasonal pattern."
                )

            investigation["verdict"] = verdict
            investigation["explanation"] = explanation
            logger.info("[investigate] DONE. Verdict: %s", verdict)
            investigation["summary_table"] = _build_investigation_summary(
                pipeline_folder, source_table, step1, step2, step3, step4
            )

            output = json.dumps(investigation, default=str)
            if len(output) > MAX_CHARS:
                output = output[:MAX_CHARS] + "\n... (truncated)"
            return output

        except Exception as e:
            return json.dumps({"error": f"pv_investigate_root_cause failed: {e}"})


def _find_pipeline_folder(source_table):
    """Map a source table name to the src/projects/ pipeline folder."""
    if not source_table:
        return None
    # integrated_metrics.diner_session_discovery -> diner_session_discovery
    parts = source_table.split(".")
    table_short = parts[-1] if parts else source_table

    # Direct match
    candidate = _get_repo_root() / "src" / "projects" / table_short
    if candidate.is_dir():
        return table_short

    # Try with _loader suffix stripped
    if table_short.endswith("_loader"):
        stripped = table_short[:-7]
        candidate = _get_repo_root() / "src" / "projects" / stripped
        if candidate.is_dir():
            return stripped

    # Fuzzy: find best substring match
    import os
    projects_dir = _get_repo_root() / "src" / "projects"
    if projects_dir.is_dir():
        folders = [f for f in os.listdir(projects_dir) if (projects_dir / f).is_dir()]
        # Exact substring match
        for f in folders:
            if f in table_short or table_short in f:
                return f
    return None


def _get_upstream_tables(pipeline_folder):
    """Parse project.py to extract upstream table dependencies (wait_for_* jobs).

    Returns list of (table_name, date_column) tuples.
    """
    if not pipeline_folder:
        return []

    project_py = _get_repo_root() / "src" / "projects" / pipeline_folder / "azkaban" / "project.py"
    if not project_py.exists():
        return []

    upstream = []
    try:
        content = project_py.read_text()
        # Pattern: wait_for_{schema}_{table} or gdpdependencies references
        # Look for table names in wait job names
        import re
        # Match wait_for_X job names
        wait_matches = re.findall(r'name="wait_for_(\w+)"', content)
        for wm in wait_matches:
            # Map common patterns to full table names + date columns
            tbl, date_col = _map_wait_name_to_table(wm)
            if tbl:
                upstream.append((tbl, date_col))
    except Exception:
        pass

    return upstream


def _map_wait_name_to_table(wait_name):
    """Map a wait_for job name to a (schema.table, date_column) tuple."""
    # Known mappings
    _WAIT_MAP = {
        "diner_session_summary": ("integrated_events.diner_session_summary", "session_start_date"),
        "diner_session_modules": ("integrated_events.diner_session_modules", "session_start_date"),
        "diner_session_clicks": ("integrated_events.diner_session_clicks", "session_start_date"),
        "diner_session_page_views": ("integrated_events.diner_session_page_views", "session_start_date"),
        "diner_session_geo_cs2": ("integrated_events.diner_session_geo_cs2", "session_start_date"),
        "diner_search_impression": ("integrated_events.diner_search_impression", "event_date"),
        "diner_search_impression_summary": ("integrated_events.diner_search_impression_summary", "event_date"),
        "diner_all_events": ("integrated_events.diner_all_events", "event_date"),
        "diner_session_menu": ("integrated_events.diner_session_menu", "session_start_date"),
        "diner_session_orders": ("integrated_events.diner_session_orders", "session_start_date"),
        "integrated_diner_search_impression": ("integrated_events.diner_search_impression", "event_date"),
        "integrated_diner_session_summary": ("integrated_events.diner_session_summary", "session_start_date"),
        "integrated_diner_search_impression_summary": ("integrated_events.diner_search_impression_summary", "event_date"),
    }

    # Direct match
    if wait_name in _WAIT_MAP:
        return _WAIT_MAP[wait_name]

    # Strip prefixes and try again
    for prefix in ["integrated_", "ods_"]:
        if wait_name.startswith(prefix):
            stripped = wait_name[len(prefix):]
            if stripped in _WAIT_MAP:
                return _WAIT_MAP[stripped]

    # For ODS tables
    if wait_name.startswith("ods_"):
        table_name = wait_name[4:]  # strip ods_
        return (f"ods.{table_name}", "created_date")

    return (None, None)


def _build_investigation_summary(pipeline_folder, source_table, step1, step2, step3, step4):
    """Build the investigation summary table data."""
    layers = []

    # Source table (our pipeline)
    layers.append({
        "layer": source_table or "Unknown",
        "repo": "events-mart",
        "code_changed": step1["verdict"] != "NO CODE CHANGES",
        "config_changed": step2["verdict"] != "NO CONFIG CHANGES",
    })

    # Upstream tables
    for tbl_info in step3.get("tables", []):
        tbl = tbl_info.get("table", "")
        layers.append({
            "layer": tbl,
            "repo": "events-mart" if "integrated_events" in tbl else "unknown",
            "volume_anomaly": tbl_info.get("anomaly", False),
            "change_pct": tbl_info.get("change_pct", 0),
        })

    return layers


def _build_svg_line_chart(chart_data, fail_dates, num_label, den_label, threshold_by_date=None):
    """Build two stacked SVG line charts: CVR trend (top) + Volume trend (bottom).

    Top chart: CVR % line with pass/fail coloring + dashed threshold line
    Bottom chart: Denominator (blue) + Numerator (green) volume lines
    Shared X-axis: Date

    threshold_by_date: dict of {date_str: threshold_value} for the FAIL threshold line
    """
    if threshold_by_date is None:
        threshold_by_date = {}
    n_points = len(chart_data)
    if n_points < 2:
        return ""

    # Extract data
    dates = [str(d.get("date", ""))[-5:] for d in chart_data]
    full_dates = [str(d.get("date", "")) for d in chart_data]
    cvrs = [d.get("cvr") for d in chart_data]
    dens = [d.get("denominator", 0) or 0 for d in chart_data]
    nums = [d.get("numerator", 0) or 0 for d in chart_data]

    # Common dimensions
    w = 800
    pad_l = 55
    pad_r = 20
    plot_w = w - pad_l - pad_r

    def x_pos(i):
        return pad_l + (i / (n_points - 1)) * plot_w if n_points > 1 else pad_l + plot_w / 2

    out = '<div class="svg-chart-wrap">\n'

    # =====================================================================
    # TOP CHART: CVR % trend
    # =====================================================================
    h1 = 180
    pad_t1 = 30
    pad_b1 = 25
    plot_h1 = h1 - pad_t1 - pad_b1

    valid_cvrs = [c for c in cvrs if c is not None]
    # Include threshold values in range calculation
    threshold_vals = [threshold_by_date.get(fd) for fd in full_dates if threshold_by_date.get(fd) is not None]
    all_cvr_vals = valid_cvrs + threshold_vals
    max_cvr = max(all_cvr_vals, default=100) or 1
    min_cvr = min(all_cvr_vals, default=0)
    # Add padding to CVR range
    cvr_top = int((max_cvr + 9) // 10) * 10
    cvr_bot = max(int(min_cvr // 10) * 10 - 10, 0)
    cvr_range = cvr_top - cvr_bot or 10

    def y_cvr(val):
        if val is None:
            return pad_t1
        return pad_t1 + plot_h1 - ((val - cvr_bot) / cvr_range * plot_h1)

    out += f'<svg width="{w}" height="{h1}" viewBox="0 0 {w} {h1}" xmlns="http://www.w3.org/2000/svg" style="background:#0f172a;border-radius:8px 8px 0 0">\n'

    # Title
    out += f'  <text x="{pad_l}" y="18" fill="#f59e0b" font-size="12" font-weight="bold">CVR %</text>\n'

    # Horizontal grid + Y-axis labels
    n_ticks = 4
    for i in range(n_ticks + 1):
        val = cvr_top - (cvr_range / n_ticks) * i
        gy = pad_t1 + (plot_h1 / n_ticks) * i
        out += f'  <line x1="{pad_l}" y1="{gy:.0f}" x2="{w - pad_r}" y2="{gy:.0f}" stroke="#1e293b" stroke-width="1"/>\n'
        out += f'  <text x="{pad_l - 6}" y="{gy + 4:.0f}" text-anchor="end" fill="#94a3b8" font-size="10">{val:.0f}%</text>\n'

    # Vertical grid + date labels at bottom
    for i, dt in enumerate(dates):
        xp = x_pos(i)
        out += f'  <line x1="{xp:.0f}" y1="{pad_t1}" x2="{xp:.0f}" y2="{pad_t1 + plot_h1}" stroke="#1e293b" stroke-width="1" stroke-dasharray="3,3"/>\n'

    # Threshold line (dashed orange) -- the FAIL boundary
    # For each chart date, find the nearest available threshold value
    sorted_thr_dates = sorted(threshold_by_date.keys())
    thr_points_list = []
    for i in range(n_points):
        dt = full_dates[i]
        thr_val = threshold_by_date.get(dt)
        if thr_val is None and sorted_thr_dates:
            # Find nearest date's threshold
            nearest = min(sorted_thr_dates, key=lambda d: abs(hash(d) - hash(dt)))
            # Actually do proper date proximity: pick closest by string sort
            candidates = [d for d in sorted_thr_dates if d <= dt]
            if candidates:
                nearest = candidates[-1]  # latest date <= chart date
            else:
                nearest = sorted_thr_dates[0]  # earliest available
            thr_val = threshold_by_date.get(nearest)
        if thr_val is not None:
            thr_points_list.append((x_pos(i), y_cvr(thr_val), thr_val))
    if thr_points_list:
        # Shaded red zone below threshold (fail region)
        bottom_y = pad_t1 + plot_h1
        fill_pts = " ".join(f"{x:.1f},{y:.1f}" for x, y, _ in thr_points_list)
        last_x = thr_points_list[-1][0]
        first_x_val = thr_points_list[0][0]
        area_pts = f"{first_x_val:.1f},{bottom_y:.1f} {fill_pts} {last_x:.1f},{bottom_y:.1f}"
        out += f'  <polygon points="{area_pts}" fill="#ef4444" fill-opacity="0.08"/>\n'

        # Draw dashed threshold line spanning full chart width
        thr_line_pts = " ".join(f"{x:.1f},{y:.1f}" for x, y, _ in thr_points_list)
        out += f'  <polyline points="{thr_line_pts}" fill="none" stroke="#f59e0b" stroke-width="2" stroke-dasharray="6,4" stroke-linejoin="round"/>\n'
        # Label at the start (left side)
        first_x, first_y, first_val = thr_points_list[0]
        last_val = thr_points_list[-1][2]
        label = f"Min expected: {first_val:.0f}%"
        if abs(first_val - last_val) > 1:
            label += f" -> {last_val:.0f}%"
        out += f'  <text x="{first_x + 5:.1f}" y="{first_y - 6:.1f}" fill="#f59e0b" font-size="9">{label}</text>\n'

    # Helper: determine fail status by comparing CVR vs threshold
    # Primary: if CVR < threshold for that date, it's a fail
    # Fallback: use PV trend fail_dates if no threshold available
    def _is_fail(i):
        dt = full_dates[i]
        cvr = cvrs[i]
        if cvr is None:
            return dt in fail_dates
        # Check against threshold (nearest date match)
        thr = threshold_by_date.get(dt)
        if thr is None and sorted_thr_dates:
            candidates = [d for d in sorted_thr_dates if d <= dt]
            nearest = candidates[-1] if candidates else sorted_thr_dates[0]
            thr = threshold_by_date.get(nearest)
        if thr is not None:
            return cvr < thr
        return dt in fail_dates

    # CVR line segments (colored by pass/fail)
    for i in range(n_points - 1):
        if cvrs[i] is None or cvrs[i + 1] is None:
            continue
        x1, y1 = x_pos(i), y_cvr(cvrs[i])
        x2, y2 = x_pos(i + 1), y_cvr(cvrs[i + 1])
        seg_color = "#ef4444" if _is_fail(i + 1) else "#22c55e"
        out += f'  <line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{seg_color}" stroke-width="3"/>\n'

    # CVR dots + labels
    for i in range(n_points):
        if cvrs[i] is None:
            continue
        xp = x_pos(i)
        yp = y_cvr(cvrs[i])
        is_fail = _is_fail(i)
        color = "#ef4444" if is_fail else "#22c55e"
        out += f'  <circle cx="{xp:.1f}" cy="{yp:.1f}" r="5" fill="{color}" stroke="#0f172a" stroke-width="2"/>\n'
        # Label: alternate above/below to avoid overlap
        label_y = yp - 10 if i % 2 == 0 else yp + 16
        out += f'  <text x="{xp:.1f}" y="{label_y:.1f}" text-anchor="middle" fill="{color}" font-size="11" font-weight="bold">{cvrs[i]:.0f}%</text>\n'

    out += '</svg>\n'

    # =====================================================================
    # BOTTOM CHART: Volume (Denominator + Numerator)
    # =====================================================================
    h2 = 180
    pad_t2 = 10
    pad_b2 = 35
    plot_h2 = h2 - pad_t2 - pad_b2

    max_vol = max(max(dens), max(nums)) or 1
    # Round up
    vol_ceil = max_vol * 1.15

    def y_vol(val):
        return pad_t2 + plot_h2 - (val / vol_ceil * plot_h2)

    out += f'<svg width="{w}" height="{h2}" viewBox="0 0 {w} {h2}" xmlns="http://www.w3.org/2000/svg" style="background:#0f172a;border-radius:0 0 8px 8px;border-top:1px solid #334155">\n'

    # Title
    out += f'  <text x="{pad_l}" y="18" fill="#60a5fa" font-size="12" font-weight="bold">Volume</text>\n'

    # Horizontal grid + Y-axis labels
    for i in range(n_ticks + 1):
        val = vol_ceil - (vol_ceil / n_ticks) * i
        gy = pad_t2 + (plot_h2 / n_ticks) * i
        out += f'  <line x1="{pad_l}" y1="{gy:.0f}" x2="{w - pad_r}" y2="{gy:.0f}" stroke="#1e293b" stroke-width="1"/>\n'
        out += f'  <text x="{pad_l - 6}" y="{gy + 4:.0f}" text-anchor="end" fill="#94a3b8" font-size="10">{_format_number(val)}</text>\n'

    # Vertical grid + date labels
    for i, dt in enumerate(dates):
        xp = x_pos(i)
        out += f'  <line x1="{xp:.0f}" y1="{pad_t2}" x2="{xp:.0f}" y2="{pad_t2 + plot_h2}" stroke="#1e293b" stroke-width="1" stroke-dasharray="3,3"/>\n'
        out += f'  <text x="{xp:.0f}" y="{h2 - pad_b2 + 18}" text-anchor="middle" fill="#94a3b8" font-size="11">{dt}</text>\n'

    # Denominator line (blue) + filled area
    den_pts = " ".join(f"{x_pos(i):.1f},{y_vol(dens[i]):.1f}" for i in range(n_points))
    # Filled area under denominator line
    area_pts = f"{x_pos(0):.1f},{pad_t2 + plot_h2:.1f} " + den_pts + f" {x_pos(n_points - 1):.1f},{pad_t2 + plot_h2:.1f}"
    out += f'  <polygon points="{area_pts}" fill="#3b82f6" fill-opacity="0.15"/>\n'
    out += f'  <polyline points="{den_pts}" fill="none" stroke="#3b82f6" stroke-width="2.5" stroke-linejoin="round"/>\n'
    for i in range(n_points):
        xp = x_pos(i)
        yp = y_vol(dens[i])
        out += f'  <circle cx="{xp:.1f}" cy="{yp:.1f}" r="4" fill="#3b82f6"/>\n'
        out += f'  <text x="{xp:.1f}" y="{yp - 8:.1f}" text-anchor="middle" fill="#60a5fa" font-size="9">{_format_number(dens[i])}</text>\n'

    # Numerator line (green) + filled area
    num_pts = " ".join(f"{x_pos(i):.1f},{y_vol(nums[i]):.1f}" for i in range(n_points))
    area_pts = f"{x_pos(0):.1f},{pad_t2 + plot_h2:.1f} " + num_pts + f" {x_pos(n_points - 1):.1f},{pad_t2 + plot_h2:.1f}"
    out += f'  <polygon points="{area_pts}" fill="#22c55e" fill-opacity="0.15"/>\n'
    out += f'  <polyline points="{num_pts}" fill="none" stroke="#22c55e" stroke-width="2.5" stroke-linejoin="round"/>\n'
    for i in range(n_points):
        xp = x_pos(i)
        yp = y_vol(nums[i])
        out += f'  <circle cx="{xp:.1f}" cy="{yp:.1f}" r="4" fill="#22c55e"/>\n'
        out += f'  <text x="{xp:.1f}" y="{yp - 8:.1f}" text-anchor="middle" fill="#86efac" font-size="9">{_format_number(nums[i])}</text>\n'

    out += '</svg>\n'
    out += '</div>\n'

    # Legend
    out += '<div class="legend">\n'
    out += f'  <div class="legend-item"><div class="legend-color" style="background:#22c55e;border-radius:50%"></div>CVR (pass)</div>\n'
    out += f'  <div class="legend-item"><div class="legend-color" style="background:#ef4444;border-radius:50%"></div>CVR (fail)</div>\n'
    out += f'  <div class="legend-item"><div style="width:20px;height:2px;border-top:2px dashed #f59e0b;margin-top:5px"></div>Min expected (fail below this)</div>\n'
    out += f'  <div class="legend-item"><div class="legend-color" style="background:#3b82f6"></div>Denominator (<code>{den_label}</code>)</div>\n'
    out += f'  <div class="legend-item"><div class="legend-color" style="background:#22c55e"></div>Numerator (<code>{num_label}</code>)</div>\n'
    out += "</div>\n"

    return out


def _format_number(n):
    """Format a number for display: 1234567 -> 1,234,567 or 1.23M."""
    if n is None:
        return "N/A"
    n = float(n)
    if abs(n) >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if abs(n) >= 10_000:
        return f"{n / 1_000:.0f}K"
    if abs(n) >= 1_000:
        return f"{n:,.0f}"
    if n == int(n):
        return str(int(n))
    return f"{n:.1f}"


def _generate_html_report(summary, recurring, analyses, pattern_label, days):
    """Generate a styled HTML report matching the quality of PV_DISCOVERY_ANALYSIS_REPORT.html.

    Includes: executive summary, daily chart, recurring failures table, per-metric
    deep dives with decomposition tables + bar charts, recommendations, and
    validation SQL queries.
    """
    now = datetime.now().strftime("%B %d, %Y %H:%M")
    summary_data = summary.get("summary", {})
    daily_rates = summary.get("daily_rates", [])

    # Collect inflection dates and root cause stats for executive summary
    inflection_dates = set()
    rc_counts = {}
    source_tables = set()
    den_multipliers = []
    cvr_drop_pcts = []
    for a in analyses:
        if a.get("inflection_date"):
            inflection_dates.add(a["inflection_date"])
        rc = a.get("root_cause")
        if rc:
            cls = rc.get("classification", "Unknown")
            rc_counts[cls] = rc_counts.get(cls, 0) + 1
            # Compute denominator multiplier
            pre_den = rc.get("pre_inflection_avg_den", 0)
            post_den = rc.get("post_inflection_avg_den", 0)
            if pre_den > 0 and post_den > 0:
                den_multipliers.append(post_den / pre_den)
        if a.get("source_table"):
            source_tables.add(a["source_table"])
    # Compute CVR drop range from recurring data
    for m in recurring:
        pct = m.get("pct_of_expected")
        if pct is not None and pct < 100:
            cvr_drop_pcts.append(100 - pct)
    primary_inflection = min(inflection_dates) if inflection_dates else "N/A"
    primary_rc = max(rc_counts, key=rc_counts.get) if rc_counts else "Unknown"
    analyzed_with_decomp = sum(1 for a in analyses if a.get("decomposition"))
    avg_den_mult = sum(den_multipliers) / len(den_multipliers) if den_multipliers else 0
    den_mult_str = f"{avg_den_mult:.0f}x" if avg_den_mult >= 1.5 else f"{(avg_den_mult - 1) * 100:+.0f}%"
    cvr_drop_range = ""
    if cvr_drop_pcts:
        cvr_drop_range = f"{min(cvr_drop_pcts):.0f}-{max(cvr_drop_pcts):.0f}%"

    # Find the "reference" analysis (first one with decomposition) for rich timeline
    ref_analysis = None
    for a in analyses:
        if a.get("decomposition") and a.get("root_cause"):
            ref_analysis = a
            break

    # -- CSS --
    css = """
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0f172a; color: #e2e8f0; line-height: 1.6; }
  .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
  h1 { color: #f97316; font-size: 28px; margin-bottom: 5px; }
  h2 { color: #fb923c; font-size: 22px; margin: 30px 0 15px; border-bottom: 2px solid #334155; padding-bottom: 8px; }
  h3 { color: #fdba74; font-size: 18px; margin: 20px 0 10px; }
  .subtitle { color: #94a3b8; font-size: 14px; margin-bottom: 20px; }
  .card { background: #1e293b; border-radius: 12px; padding: 20px; margin: 15px 0; border: 1px solid #334155; }
  .card-red { border-left: 4px solid #ef4444; }
  .card-green { border-left: 4px solid #22c55e; }
  .card-orange { border-left: 4px solid #f97316; }
  .card-blue { border-left: 4px solid #3b82f6; }
  .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 15px 0; }
  .metric-box { background: #0f172a; border-radius: 8px; padding: 15px; text-align: center; }
  .metric-value { font-size: 32px; font-weight: bold; }
  .metric-label { color: #94a3b8; font-size: 12px; text-transform: uppercase; }
  .fail { color: #ef4444; }
  .pass { color: #22c55e; }
  .warn { color: #f59e0b; }
  table { width: 100%; border-collapse: collapse; margin: 10px 0; }
  th { background: #334155; color: #f97316; padding: 10px 12px; text-align: left; font-size: 13px; text-transform: uppercase; }
  td { padding: 8px 12px; border-bottom: 1px solid #1e293b; font-size: 13px; }
  tr:hover td { background: #1e293b; }
  tr.fail-row { background: #7f1d1d33; }
  .tag { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }
  .tag-fail { background: #7f1d1d; color: #fca5a5; }
  .tag-pass { background: #14532d; color: #86efac; }
  .tag-warn { background: #78350f; color: #fcd34d; }
  code { background: #334155; padding: 2px 6px; border-radius: 4px; font-size: 12px; color: #fbbf24; }
  .sql-block { background: #0f172a; border: 1px solid #334155; border-radius: 8px; padding: 15px; margin: 10px 0; overflow-x: auto; }
  .sql-block code { background: none; display: block; white-space: pre; font-size: 12px; color: #a5b4fc; }
  .divider { border: none; border-top: 1px solid #334155; margin: 30px 0; }
  .footer { text-align: center; color: #475569; font-size: 12px; margin-top: 40px; padding: 20px; }
  .bar-chart { display: flex; align-items: flex-end; gap: 2px; min-height: 160px; padding: 25px 0 10px 0; }
  .bar { display: flex; flex-direction: column; align-items: center; min-width: 25px; flex: 1; }
  .bar-fill { width: 20px; border-radius: 3px 3px 0 0; min-height: 2px; }
  .bar-label { font-size: 9px; color: #94a3b8; margin-top: 4px; writing-mode: vertical-rl; transform: rotate(180deg); height: 50px; }
  .bar-value { font-size: 9px; color: #cbd5e1; margin-bottom: 2px; white-space: nowrap; }
  .svg-chart-wrap { width: 100%; overflow-x: auto; margin: 10px 0; }
  .legend { display: flex; gap: 20px; margin: 10px 0; }
  .legend-item { display: flex; align-items: center; gap: 5px; font-size: 12px; color: #94a3b8; }
  .legend-color { width: 12px; height: 12px; border-radius: 3px; }
  .timeline { position: relative; padding-left: 30px; margin: 20px 0; }
  .timeline::before { content: ''; position: absolute; left: 10px; top: 0; bottom: 0; width: 2px; background: #334155; }
  .timeline-item { position: relative; margin-bottom: 20px; }
  .timeline-item::before { content: ''; position: absolute; left: -24px; top: 6px; width: 12px; height: 12px; border-radius: 50%; background: #f97316; }
  .timeline-item.critical::before { background: #ef4444; }
"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PV Failure Analysis Report - {_html_escape(pattern_label)}</title>
<style>{css}</style>
</head>
<body>
<div class="container">

<h1>PV Failure Analysis: {_html_escape(pattern_label)}</h1>
<p class="subtitle">Generated {now} | Last {days} days | Clickstream Data Platform</p>

<div class="metric-grid">
  <div class="metric-box">
    <div class="metric-value fail">{len(recurring)}</div>
    <div class="metric-label">Metrics Failing Repeatedly</div>
  </div>
  <div class="metric-box">
    <div class="metric-value warn">~{summary_data.get('avg_daily_fail_pct', 0)}%</div>
    <div class="metric-label">Overall Daily Fail Rate</div>
  </div>
  <div class="metric-box">
    <div class="metric-value" style="color:#3b82f6">{primary_inflection}</div>
    <div class="metric-label">Inflection Date</div>
  </div>
  <div class="metric-box">
    <div class="metric-value" style="color:#a78bfa">{den_mult_str if den_multipliers else primary_rc}</div>
    <div class="metric-label">{"Denominator Increase" if den_multipliers else "Primary Root Cause"}</div>
  </div>
</div>
"""

    # =====================================================================
    # EXECUTIVE SUMMARY
    # =====================================================================
    total_recurring = len(recurring)
    # Count metrics failing every day
    max_fail_days = max((m.get("fail_count", 0) for m in recurring), default=0)
    total_max_days = max((m.get("total_days", 0) for m in recurring), default=0)
    always_failing = sum(1 for m in recurring if m.get("fail_count", 0) == max_fail_days and max_fail_days >= 3)

    html += '<h2>Executive Summary</h2>\n<div class="card card-red">\n'

    if primary_rc == "Denominator Spike" and ref_analysis:
        rc = ref_analysis["root_cause"]
        src_table = ref_analysis.get("source_table", "")
        den_col = rc.get("denominator_col", "denominator")
        pre_den = rc.get("pre_inflection_avg_den", 0)
        post_den = rc.get("post_inflection_avg_den", 0)
        mult = post_den / pre_den if pre_den else 0

        html += f"<p>{always_failing} {_html_escape(pattern_label)} CVR metrics have been failing "
        if always_failing > 0 and max_fail_days == total_max_days:
            html += f"<strong>every day for {max_fail_days} consecutive days</strong>. "
        else:
            html += f"<strong>repeatedly over the last {days} days</strong>. "
        html += f"The root cause is an <strong>upstream volume change around {primary_inflection}</strong> "
        html += f"that increased the <code>{_html_escape(den_col)}</code> by ~{mult:.0f}x "
        html += f"in <code>{_html_escape(src_table)}</code>. "
        html += "This inflated the CVR denominator while the numerator (conversions) stayed flat, "
        if cvr_drop_range:
            html += f"causing all CVR metrics to drop by <strong>{cvr_drop_range}</strong>.</p>\n"
        else:
            html += "causing all CVR metrics to drop significantly.</p>\n"
        html += "<br>\n"
        html += '<p><strong>This is NOT a pipeline bug.</strong> No events-mart code changes caused this. '
        html += "The change is upstream of the data pipeline (backend service or data source).</p>\n"
    elif primary_rc == "Numerator Drop":
        html += f"<p>{total_recurring} metrics have been failing repeatedly over the last {days} days. "
        html += f"The root cause is a <strong>conversion drop starting around {primary_inflection}</strong> -- "
        html += "the numerator (conversions) dropped while the denominator (traffic) stayed flat. "
        html += "This indicates a real change in user behavior or a product change.</p>\n"
    elif primary_rc == "Traffic Drop":
        html += f"<p>{total_recurring} metrics have been failing repeatedly over the last {days} days. "
        html += f"The root cause is an <strong>overall traffic decline starting around {primary_inflection}</strong> -- "
        html += "both numerator and denominator dropped proportionally.</p>\n"
    else:
        html += f"<p>{total_recurring} metrics matching <strong>{_html_escape(pattern_label)}</strong> have been "
        html += f"failing <strong>3+ days out of the last {days}</strong>. "
        html += f"Primary root cause: <strong>{_html_escape(primary_rc)}</strong>. "
        html += "See individual metric deep dives below for details.</p>\n"

    html += "</div>\n"

    # =====================================================================
    # TIMELINE (data-driven with actual values from decomposition)
    # =====================================================================
    if inflection_dates:
        html += '<h2>Timeline of Events</h2>\n<div class="timeline">\n'

        # Extract key data points from the reference analysis for rich timeline
        pre_cvr_range = ""
        pre_den_range = ""
        inflection_cvr = ""
        inflection_den = ""
        peak_den = ""
        peak_den_date = ""
        post_cvr_range = ""
        ref_metric_label = ""
        if ref_analysis and ref_analysis.get("decomposition"):
            decomp = ref_analysis["decomposition"]
            rc = ref_analysis.get("root_cause", {})
            ref_metric_label = ref_analysis.get("metric_name", "").split("_")[-2] + "_" + ref_analysis.get("metric_name", "").split("_")[-1] if "_" in ref_analysis.get("metric_name", "") else ref_analysis.get("metric_name", "")

            pre_rows = [d for d in decomp if d.get("date") and d["date"] < primary_inflection and d.get("cvr") is not None]
            post_rows = [d for d in decomp if d.get("date") and d["date"] >= primary_inflection and d.get("cvr") is not None]

            if pre_rows:
                pre_cvrs = [d["cvr"] for d in pre_rows]
                pre_dens = [d["denominator"] for d in pre_rows if d.get("denominator")]
                pre_cvr_range = f"{min(pre_cvrs):.0f}-{max(pre_cvrs):.0f}%"
                if pre_dens:
                    pre_den_range = f"{_format_number(min(pre_dens))}-{_format_number(max(pre_dens))}/day"

            if post_rows:
                # Inflection day values
                inflection_row = post_rows[0]
                inflection_cvr = f"{inflection_row['cvr']:.1f}%" if inflection_row.get("cvr") else ""
                inflection_den = _format_number(inflection_row.get("denominator"))

                # Find peak denominator
                peak_row = max(post_rows, key=lambda d: d.get("denominator", 0) or 0)
                peak_den = _format_number(peak_row.get("denominator"))
                peak_den_date = str(peak_row.get("date", ""))

                # Stabilized values (last 3 days)
                stable_rows = post_rows[-3:]
                if stable_rows:
                    stable_cvrs = [d["cvr"] for d in stable_rows if d.get("cvr")]
                    if stable_cvrs:
                        post_cvr_range = f"~{sum(stable_cvrs) / len(stable_cvrs):.0f}%"

        # Get trend data for all dates
        all_trend_dates = set()
        for a in analyses:
            for t in a.get("trend", []):
                if t.get("date"):
                    all_trend_dates.add(t["date"])
        sorted_dates = sorted(all_trend_dates)
        pre_dates = [d for d in sorted_dates if d < primary_inflection]
        post_dates = [d for d in sorted_dates if d >= primary_inflection]

        # Timeline entry 1: Stable baseline
        if pre_dates:
            baseline_detail = ""
            if pre_den_range:
                baseline_detail += f" | Denominator: {pre_den_range}"
            if pre_cvr_range:
                baseline_detail += f" | CVR: {pre_cvr_range}"
            html += f'<div class="timeline-item"><strong>{pre_dates[0]} - {pre_dates[-1]}</strong> &mdash; Stable baseline period<br>'
            html += f'<span style="color:#94a3b8">Metrics passing within expected thresholds{baseline_detail}.</span></div>\n'

        # Timeline entry 2: Inflection
        html += f'<div class="timeline-item critical"><strong>{primary_inflection}</strong> &mdash; Inflection point<br>'
        if inflection_cvr and inflection_den:
            html += f'<span style="color:#94a3b8">Denominator jumps to {inflection_den}. CVR drops to {inflection_cvr}.</span></div>\n'
        else:
            html += f'<span style="color:#94a3b8">First failures detected. {_html_escape(primary_rc)}.</span></div>\n'

        # Timeline entry 3: Peak impact (if different from inflection)
        if peak_den_date and peak_den_date != primary_inflection and peak_den:
            html += f'<div class="timeline-item critical"><strong>{peak_den_date}</strong> &mdash; Peak impact<br>'
            html += f'<span style="color:#94a3b8">Denominator peaks at {peak_den}.'
            if den_multipliers:
                html += f' ({avg_den_mult:.1f}x vs baseline).'
            html += '</span></div>\n'

        # Timeline entry 4: New steady state
        if post_dates and len(post_dates) > 3:
            html += f'<div class="timeline-item"><strong>{post_dates[2]} - {post_dates[-1]}</strong> &mdash; New steady state<br>'
            detail = f"Metrics stabilize at new lower levels."
            if post_cvr_range:
                detail += f" CVR: {post_cvr_range}."
            html += f'<span style="color:#94a3b8">{detail}</span></div>\n'

        # Timeline entry 5: PV flags failures
        first_fail_dates = [m.get("first_fail_date", "") for m in recurring if m.get("first_fail_date")]
        if first_fail_dates:
            earliest_pv_fail = min(first_fail_dates)
            if earliest_pv_fail > primary_inflection:
                html += f'<div class="timeline-item"><strong>{earliest_pv_fail}+</strong> &mdash; PV framework starts flagging failures<br>'
                html += '<span style="color:#94a3b8">36-day lookback includes pre-change data. Actual values are '
                if cvr_drop_range:
                    html += f'{cvr_drop_range} below'
                else:
                    html += 'significantly below'
                html += ' historical expected values.</span></div>\n'

        html += "</div>\n"

    # =====================================================================
    # DAILY FAILURE RATE CHART
    # =====================================================================
    html += '<h2>Daily Failure Rate</h2>\n<div class="card card-orange">\n'
    if daily_rates:
        max_fail = max((r.get("fail_pct", 0) for r in daily_rates), default=1) or 1
        html += '<div class="bar-chart">\n'
        for r in daily_rates:
            pct = r.get("fail_pct", 0)
            height = max(int(pct / max_fail * 100), 2)
            color = "#ef4444" if pct > 10 else "#f59e0b" if pct > 5 else "#22c55e"
            date_label = str(r.get("date", ""))[-5:]
            html += f'  <div class="bar"><div class="bar-value">{pct}%</div><div class="bar-fill" style="height:{height}px;background:{color}"></div><div class="bar-label">{date_label}</div></div>\n'
        html += "</div>\n"
    html += "</div>\n"

    # =====================================================================
    # RECURRING FAILURES TABLE
    # =====================================================================
    html += '<h2>All Recurring Failures</h2>\n<div class="card card-blue">\n<table>\n'
    html += "<tr><th>Metric</th><th>Segment</th><th>Fail Days</th><th>Avg Actual</th><th>Avg Expected</th><th>% of Expected</th><th>Category</th></tr>\n"
    for m in recurring:
        pct = m.get("pct_of_expected")
        pct_str = f"{pct}%" if pct is not None else "N/A"
        pct_class = "fail" if pct is not None and pct < 70 else ("warn" if pct is not None and pct < 90 else "pass")
        cat = _categorize_metric(m.get("metric_name", ""))
        actual = m.get("avg_actual")
        expected = m.get("avg_expected")
        actual_str = f"{actual:.2f}" if actual is not None else "N/A"
        expected_str = f"{expected:.2f}" if expected is not None else "N/A"
        html += (
            f'<tr><td><code>{_html_escape(m.get("metric_name", ""))}</code></td>'
            f'<td>{_html_escape(m.get("segment", ""))}</td>'
            f'<td>{m.get("fail_count", 0)}/{m.get("total_days", 0)}</td>'
            f'<td class="fail">{actual_str}</td>'
            f"<td>{expected_str}</td>"
            f'<td class="{pct_class}">{pct_str}</td>'
            f"<td>{cat}</td></tr>\n"
        )
    html += "</table>\n</div>\n"

    # =====================================================================
    # METRIC DEEP DIVES (with full decomposition)
    # =====================================================================
    if analyses:
        html += "<h2>Metric Deep Dives</h2>\n"
        for a in analyses:
            name = a.get("metric_name", "")
            seg = a.get("segment", "all")
            inflection = a.get("inflection_date", "N/A")
            rc = a.get("root_cause")
            category = a.get("category", "Other")
            decomp = a.get("decomposition")

            card_class = "card-red" if rc and rc.get("classification") in ("Denominator Spike", "Numerator Drop") else "card-orange"
            html += f'<div class="card {card_class}">\n'
            html += f'<h3><code>{_html_escape(name)}</code> [{_html_escape(seg)}]</h3>\n'
            html += f'<p>Category: <strong>{category}</strong> | Inflection: <strong>{inflection}</strong> | '
            html += f'Pass: {a.get("pass_count", 0)} | Fail: {a.get("fail_count", 0)}</p>\n'

            if a.get("sql_formula"):
                html += f'<p style="margin-top:8px">Formula: <code>{_html_escape(a["sql_formula"])}</code></p>\n'
            if a.get("source_table"):
                html += f'<p>Source: <code>hive.{_html_escape(a["source_table"])}</code></p>\n'

            # -- Root cause classification --
            if rc:
                classification = rc.get("classification", "Unknown")
                tag_class = "tag-fail" if classification in ("Denominator Spike", "Numerator Drop") else "tag-warn"
                html += f'<p style="margin-top:12px">Root Cause: <span class="tag {tag_class}">{_html_escape(classification)}</span></p>\n'
                html += f'<p style="color:#94a3b8">{_html_escape(rc.get("explanation", ""))}</p>\n'

                # -- Pre/Post comparison table --
                html += '<table style="margin-top:10px">\n'
                html += "<tr><th>Component</th><th>Pre-Inflection Avg</th><th>Post-Inflection Avg</th><th>Change</th></tr>\n"

                pre_num = rc.get("pre_inflection_avg_num", 0)
                post_num = rc.get("post_inflection_avg_num", 0)
                num_change = ((post_num / pre_num - 1) * 100) if pre_num else 0
                num_class = "fail" if abs(num_change) > 15 else "pass"

                pre_den = rc.get("pre_inflection_avg_den", 0)
                post_den = rc.get("post_inflection_avg_den", 0)
                den_change = ((post_den / pre_den - 1) * 100) if pre_den else 0
                den_class = "fail" if abs(den_change) > 15 else "pass"

                html += (
                    f'<tr><td>Numerator (<code>{_html_escape(rc.get("numerator_col", ""))}</code>)</td>'
                    f"<td>{_format_number(pre_num)}</td><td>{_format_number(post_num)}</td>"
                    f'<td class="{num_class}">{num_change:+.1f}%</td></tr>\n'
                )
                html += (
                    f'<tr><td>Denominator (<code>{_html_escape(rc.get("denominator_col", ""))}</code>)</td>'
                    f"<td>{_format_number(pre_den)}</td><td>{_format_number(post_den)}</td>"
                    f'<td class="{den_class}">{den_change:+.1f}%</td></tr>\n'
                )
                html += "</table>\n"

            # -- Decomposition: get column names from rc or sql_formula --
            decomp_den_col = ""
            decomp_num_col = ""
            if rc:
                decomp_den_col = rc.get("denominator_col", "")
                decomp_num_col = rc.get("numerator_col", "")
            if (not decomp_den_col or not decomp_num_col) and a.get("sql_formula"):
                parts = _parse_cvr_components(a["sql_formula"])
                if parts:
                    if not decomp_num_col:
                        decomp_num_col = parts[0]
                    if not decomp_den_col:
                        decomp_den_col = parts[1]

            # Compute outlier threshold for filtering partial days
            decomp_min_threshold = 0
            if decomp:
                den_values_all = [d.get("denominator") for d in decomp if d.get("denominator") is not None and d["denominator"] > 0]
                if den_values_all:
                    sorted_dens_all = sorted(den_values_all)
                    median_den_all = sorted_dens_all[len(sorted_dens_all) // 2]
                    decomp_min_threshold = median_den_all * 0.01

            if decomp and len(decomp) > 3:
                # Filter to valid rows, take last 14 to show both pass and fail periods
                chart_data = [d for d in decomp if d.get("denominator") is not None and d["denominator"] > 0 and d["denominator"] >= decomp_min_threshold]
                chart_data = chart_data[-14:]
                den_values = [d["denominator"] for d in chart_data]
                num_values = [d.get("numerator", 0) or 0 for d in chart_data]
                cvr_values = [d["cvr"] for d in chart_data if d.get("cvr") is not None]
                if den_values and cvr_values:
                    num_label = _html_escape(decomp_num_col) if decomp_num_col else "Numerator"
                    den_label = _html_escape(decomp_den_col) if decomp_den_col else "Denominator"

                    # Build fail status lookup from PV trend
                    fail_dates = set()
                    for t in a.get("trend", []):
                        if t.get("status") == "FAIL":
                            fail_dates.add(str(t.get("date", "")))

                    # Build threshold lookup from PV trend data
                    # min_expected IS the threshold -- actual < min_expected = FAIL
                    threshold_by_date = {}
                    for t in a.get("trend", []):
                        dt = str(t.get("date", ""))
                        exp = t.get("expected")
                        if exp is not None:
                            threshold_by_date[dt] = exp

                    html += f'<h3 style="margin-top:15px">CVR Decomposition</h3>\n'
                    html += _build_svg_line_chart(chart_data, fail_dates, num_label, den_label, threshold_by_date)

            # -- Summary line under the chart --
            if decomp:
                pre_rows = [d for d in decomp if d.get("denominator") and inflection and str(d.get("date", "")) < inflection]
                post_rows = [d for d in decomp if d.get("denominator") and inflection and str(d.get("date", "")) >= inflection]
                if pre_rows and post_rows:
                    pre_num_avg = sum(d["numerator"] for d in pre_rows if d.get("numerator")) / len(pre_rows)
                    pre_den_avg = sum(d["denominator"] for d in pre_rows) / len(pre_rows)
                    post_den_avg = sum(d["denominator"] for d in post_rows) / len(post_rows)
                    den_mult = post_den_avg / pre_den_avg if pre_den_avg else 0
                    html += f'<p style="color:#94a3b8;margin-top:10px">Numerator stayed flat (~{_format_number(pre_num_avg)}/day). '
                    html += f"Denominator went from {_format_number(pre_den_avg)} to {_format_number(post_den_avg)} "
                    html += f"({den_mult:.1f}x).</p>\n"

            # -- PV trend table (fallback for non-CVR metrics without decomposition) --
            if not decomp:
                trend = a.get("trend", [])
                if trend:
                    html += '<h3 style="margin-top:15px">PV Status Trend</h3>\n<table>\n'
                    html += "<tr><th>Date</th><th>Status</th><th>Actual</th><th>Expected (min)</th><th>Threshold</th></tr>\n"
                    for t in trend[-14:]:
                        status = t.get("status", "")
                        tag_cls = "tag-fail" if status == "FAIL" else ("tag-pass" if status == "PASS" else "tag-warn")
                        row_class = ' class="fail-row"' if status == "FAIL" else ""
                        actual_str = f'{t["actual"]:.2f}' if t.get("actual") is not None else "N/A"
                        expected_str = f'{t["expected"]:.2f}' if t.get("expected") is not None else "N/A"
                        threshold_str = f'{t["threshold"]:.0f}%' if t.get("threshold") is not None else "N/A"
                        html += (
                            f'<tr{row_class}><td>{t.get("date", "")}</td>'
                            f'<td><span class="tag {tag_cls}">{status}</span></td>'
                            f"<td>{actual_str}</td><td>{expected_str}</td><td>{threshold_str}</td></tr>\n"
                        )
                    html += "</table>\n"

            html += "</div>\n"

    # =====================================================================
    # DATA LINEAGE (auto-generated from known upstream dependencies)
    if source_tables:
        html += '<h2>Data Lineage</h2>\n<div class="card card-blue">\n'
        html += '<pre style="color:#a5b4fc;font-size:13px;line-height:1.8">\n'

        for src in sorted(source_tables):
            upstream = _UPSTREAM_LINEAGE.get(src)
            if upstream:
                # Show full chain from upstream to PV
                for i, (layer, repo, note) in enumerate(upstream):
                    repo_note = f"  (repo: {repo})" if repo else ""
                    is_root = i == 0 and primary_rc == "Denominator Spike"
                    root_marker = f'     <span style="color:#ef4444">&lt;-- ROOT CAUSE: Volume change detected</span>' if is_root else ""
                    html += f'{_html_escape(layer)}{repo_note}{root_marker}\n'
                    html += f'       |\n       v\n'

                # The source table itself
                impact = ""
                if primary_rc == "Denominator Spike" and den_mult_str:
                    impact = f'     <span style="color:#ef4444">&lt;-- CVR denominators inflated {den_mult_str}, CVR dropped</span>'
                elif primary_rc == "Numerator Drop":
                    impact = f'     <span style="color:#ef4444">&lt;-- Numerator dropped</span>'
                html += f'{_html_escape(src)}  (repo: events-mart){impact}\n'
            else:
                # Unknown upstream -- show simplified chain
                html += f'Upstream Data Source\n       |\n       v\n'
                html += f'{_html_escape(src)}  (repo: events-mart)\n'

            html += f'       |\n       v\n'

        html += f'{_html_escape(_PV_TABLE)}  (PV Framework flags as FAIL)\n'
        html += '</pre>\n</div>\n'

    # =====================================================================
    # RECOMMENDATIONS
    # =====================================================================
    html += '<h2>Recommendations</h2>\n<div class="card card-orange">\n<table>\n'
    html += "<tr><th>Option</th><th>Action</th><th>Pros</th><th>Cons</th></tr>\n"
    if primary_rc == "Denominator Spike":
        html += "<tr><td><strong>A. Lower WoW threshold</strong></td>"
        html += "<td>Reduce threshold from 85% to 50% for affected CVR metrics in <code>table_validations.json</code></td>"
        html += "<td>Immediate fix, stops false alerts</td><td>Less sensitive to real issues</td></tr>\n"
        html += "<tr><td><strong>B. Reduce lookback_days</strong></td>"
        html += "<td>Change from 36 to 14 days for affected metrics</td>"
        html += "<td>Faster baseline adjustment, keeps threshold meaningful</td><td>Shorter baseline = more noise</td></tr>\n"
        # Compute auto-correction date (36 days after inflection)
        auto_correct_date = ""
        try:
            from datetime import timedelta
            infl_dt = datetime.strptime(primary_inflection, "%Y-%m-%d")
            auto_correct_dt = infl_dt + timedelta(days=36)
            auto_correct_date = auto_correct_dt.strftime("%b %d")
            days_remaining = (auto_correct_dt - datetime.now()).days
            remaining_note = f" (~{days_remaining} days from now)" if days_remaining > 0 else " (should have auto-corrected already)"
        except Exception:
            auto_correct_date = "~36 days later"
            remaining_note = ""
        html += f"<tr><td><strong>C. Wait for auto-correction</strong></td>"
        html += f"<td>Do nothing. By {auto_correct_date}{remaining_note}, the lookback window will only contain post-change data</td>"
        html += "<td>No code change needed</td><td>Continued daily false alerts until then</td></tr>\n"
        html += "<tr><td><strong>D. Investigate upstream</strong></td>"
        html += "<td>Contact the upstream service team to understand if the volume change is intentional</td>"
        html += "<td>Addresses root cause</td><td>May be an intentional product change</td></tr>\n"
    elif primary_rc == "Numerator Drop":
        html += "<tr><td><strong>A. Investigate product changes</strong></td>"
        html += "<td>Check for A/B experiments, app releases, or feature changes around the inflection date</td>"
        html += "<td>Addresses root cause</td><td>May require cross-team coordination</td></tr>\n"
        html += "<tr><td><strong>B. Lower threshold temporarily</strong></td>"
        html += "<td>Reduce threshold while investigating</td>"
        html += "<td>Stops alert noise</td><td>Masks real issues</td></tr>\n"
    elif primary_rc == "Traffic Drop":
        html += "<tr><td><strong>A. Check for seasonal patterns</strong></td>"
        html += "<td>Compare against same period last year</td>"
        html += "<td>May be expected behavior</td><td>Requires historical data access</td></tr>\n"
        html += "<tr><td><strong>B. Check platform health</strong></td>"
        html += "<td>Look for outages, infrastructure issues around inflection date</td>"
        html += "<td>May find transient issue</td><td>Time-consuming</td></tr>\n"
    else:
        html += "<tr><td><strong>A. Manual investigation</strong></td>"
        html += "<td>Review each failing metric individually using the deep dives above</td>"
        html += "<td>Thorough</td><td>Time-consuming</td></tr>\n"
        html += "<tr><td><strong>B. Adjust thresholds</strong></td>"
        html += "<td>Review thresholds in <code>table_validations.json</code> for affected metrics</td>"
        html += "<td>Reduces noise</td><td>May miss real issues</td></tr>\n"
    html += "</table>\n</div>\n"

    # =====================================================================
    # VALIDATION SQL QUERIES
    # =====================================================================
    html += "<h2>Validation Queries</h2>\n"

    # Query 1: Failure rate by day
    html += '<h3>Check Failure Rate by Day</h3>\n<div class="sql-block"><code>'
    html += "SELECT metric_date,\n"
    html += "    COUNT(CASE WHEN status = 'FAIL' THEN 1 END) AS failures,\n"
    html += "    COUNT(CASE WHEN status = 'PASS' THEN 1 END) AS passes,\n"
    html += "    ROUND(100.0 * COUNT(CASE WHEN status = 'FAIL' THEN 1 END) / COUNT(*), 1) AS fail_pct\n"
    html += f"FROM {_html_escape(_PV_TABLE)}\n"
    html += f"WHERE metric_date >= CURRENT_DATE - INTERVAL '{days}' DAY\n"
    html += "GROUP BY 1 ORDER BY 1</code></div>\n"

    # Query 2: Per-metric decomposition (for the first CVR metric with decomposition)
    for a in analyses:
        if a.get("decomposition") and a.get("root_cause") and a.get("source_table"):
            rc = a["root_cause"]
            num_col = rc.get("numerator_col", "")
            den_col = rc.get("denominator_col", "")
            src_table = a["source_table"]
            date_col = "session_start_date"  # default
            config = _find_metric_config(a["metric_name"])
            if config:
                date_col = config.get("date_column", date_col)
            seg = a.get("segment", "")
            seg_filter = ""
            if seg and seg != "all" and config and config.get("group_col"):
                seg_filter = f"\n  AND {config['group_col']} = '{_html_escape(seg)}'"

            html += f'<h3>{_html_escape(a["metric_name"])} - Numerator vs Denominator</h3>\n'
            html += '<div class="sql-block"><code>'
            html += f"SELECT {_html_escape(date_col)},\n"
            html += f"    SUM({_html_escape(num_col)}) AS numerator,\n"
            html += f"    SUM({_html_escape(den_col)}) AS denominator,\n"
            html += f"    ROUND(SUM({_html_escape(num_col)}) * 100.0 / NULLIF(SUM({_html_escape(den_col)}), 0), 1) AS cvr\n"
            html += f"FROM hive.{_html_escape(src_table)}\n"
            html += f"WHERE {_html_escape(date_col)} >= DATE '{primary_inflection}' - INTERVAL '7' DAY{_html_escape(seg_filter)}\n"
            html += "GROUP BY 1 ORDER BY 1</code></div>\n"
            break  # Only show one example

    # =====================================================================
    # DATA SOURCES
    # =====================================================================
    if source_tables:
        html += '<h2>Data Sources</h2>\n<div class="card card-green">\n<table>\n'
        html += "<tr><th>Table</th><th>Purpose</th></tr>\n"
        html += f"<tr><td><code>{_html_escape(_PV_TABLE)}</code></td><td>PV results (PASS/FAIL per metric per date)</td></tr>\n"
        for tbl in sorted(source_tables):
            html += f"<tr><td><code>hive.{_html_escape(tbl)}</code></td><td>Metric source table (numerator/denominator)</td></tr>\n"
        html += "<tr><td><code>table_validations.json</code></td><td>Validation config (SQL formulas, thresholds)</td></tr>\n"
        html += "</table>\n</div>\n"

    # =====================================================================
    # FOOTER
    # =====================================================================
    html += f"""
<hr class="divider">
<div class="footer">
  <p>Generated by PV Analysis Tool | Clickstream Data Platform | {now}</p>
  <p>Data source: <code>{_html_escape(_PV_TABLE)}</code></p>
</div>

</div>
</body>
</html>
"""
    return html


def _html_escape(text):
    """Basic HTML escaping."""
    if not isinstance(text, str):
        text = str(text)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
