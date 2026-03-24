"""Sync plan calculator for GDP MCP server.

Calculates which upstream tables need syncing from prod to dev
and their date ranges, derived from each project's code.
"""

import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Per-project sync configurations.
# Offsets are derived from WHERE clauses in each project's loader code.
# start_offset/end_offset are days subtracted/added relative to
# start_date and end_date respectively.
SYNC_CONFIGS = {
    "diner_session_topics": {
        "default_num_days_load": 3,
        "azkaban_project": "events-mart_diner_session_topics_loader",
        "azkaban_flow": "diner_session_topics_cleanup",
        "tables": [
            {"table": "integrated_events.diner_all_events", "partition_col": "event_date", "start_offset": 0, "end_offset": 0},
            {"table": "integrated_events.diner_session_summary", "partition_col": "session_start_date", "start_offset": 0, "end_offset": 0},
            {"table": "integrated_events.diner_search_impression", "partition_col": "event_date", "start_offset": 0, "end_offset": 0},
            {"table": "integrated_events.diner_search_impression_summary", "partition_col": "session_start_date", "start_offset": 2, "end_offset": 2},
            {"table": "ods.topics_list", "partition_col": "created_date", "start_offset": 5, "end_offset": 5},
            {"table": "ods.topics_search_type", "partition_col": "created_date", "start_offset": 5, "end_offset": 5},
            {"table": "ods.topics_content", "partition_col": "created_date", "start_offset": 5, "end_offset": 5},
            {"table": "ods.diner_sponsored_search_impression", "partition_col": "created_date", "start_offset": 4, "end_offset": 2},
            {"table": "ods.ddml_control_plane_resolve_flow_event", "partition_col": "created_date", "start_offset": 5, "end_offset": 5},
            {"table": "gdp_impressions.last_click_attributions", "partition_col": "event_date", "start_offset": 5, "end_offset": 5},
            {"table": "integrated_restaurant.merchant_dim", "partition_col": None, "static": True},
        ],
    },
    "clickstream_pv_metric_analysis": {
        "default_num_days_load": 0,
        "azkaban_project": "events-mart_clickstream_pv_metric_analysis_loader",
        "azkaban_flow": "clickstream_pv_metric_analysis_loader",
        "tables": [
            # WoW validation tables (36-day lookback)
            {"table": "integrated_metrics.diner_session_discovery", "partition_col": "session_start_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.diner_session_menu", "partition_col": "event_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_metrics.diner_session_menu_metrics", "partition_col": "session_start_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.diner_ad_impressions", "partition_col": "session_start_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.diner_session_topics", "partition_col": "event_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.diner_session_topics_impressions", "partition_col": "event_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.diner_session_cart_action", "partition_col": "event_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.diner_session_geo_cs2", "partition_col": "session_start_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.diner_session_cross_sell_impressions", "partition_col": "event_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.diner_session_funnel", "partition_col": "session_start_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.restaurant_search_impression_summary", "partition_col": "session_start_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.diner_session_clicks", "partition_col": "event_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.topic_info_exchange", "partition_col": "event_date", "start_offset": 36, "end_offset": 0},
            {"table": "validate_data.ads_recon", "partition_col": "daily_date", "start_offset": 30, "end_offset": 0},
            {"table": "integrated_history_restaurant.merchant_dim", "partition_col": "load_dt", "start_offset": 36, "end_offset": 0, "note": "integer format yyyyMMdd"},
            {"table": "integrated_events.diner_search_impression_summary", "partition_col": "session_start_date", "start_offset": 36, "end_offset": 0},
            # Additional upstream tables from Azkaban wait jobs
            {"table": "integrated_events.diner_all_events", "partition_col": "event_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.diner_session_summary", "partition_col": "session_start_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.diner_session_orders", "partition_col": "session_start_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.diner_invalid_sessions", "partition_col": "session_start_date", "start_offset": 36, "end_offset": 0},
            {"table": "integrated_events.diner_search_impression", "partition_col": "event_date", "start_offset": 36, "end_offset": 0},
            # Static/dimension tables (no date partitioning needed)
            {"table": "integrated_core.order_fact", "partition_col": None, "static": True},
            {"table": "integrated_corporate.corp_diner_dim", "partition_col": None, "static": True},
            {"table": "integrated_diner.diner_dim", "partition_col": None, "static": True},
            {"table": "integrated_restaurant.merchant_dim", "partition_col": None, "static": True},
        ],
    },
    "diner_session_summary": {
        "default_num_days_load": 1,
        "azkaban_project": "events-mart_clickstream_v2_events_process",
        "azkaban_flow": "clickstream_v2_diner_all_events_etl_flow",
        "tables": [
            {"table": "integrated_events.diner_all_events", "partition_col": "event_date", "start_offset": 0, "end_offset": 0},
            {"table": "integrated_events.diner_session_summary", "partition_col": "session_start_date", "start_offset": 0, "end_offset": 0},
            {"table": "integrated_events.diner_invalid_sessions", "partition_col": "session_start_date", "start_offset": 0, "end_offset": 0},
        ],
    },
}


def _calculate_plan(project_name, run_date_str, num_days_load):
    """Compute sync ranges for all tables in a project."""
    config = SYNC_CONFIGS[project_name]
    ndl = num_days_load if num_days_load > 0 else config["default_num_days_load"]

    run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date()
    start_date = run_date - timedelta(days=ndl)
    end_date = run_date

    rows = []
    for t in config["tables"]:
        table = t["table"]
        if t.get("static"):
            rows.append({
                "table": table,
                "partition_col": "(static)",
                "sync_start": "-",
                "sync_end": "-",
                "num_partitions": "skip",
            })
        else:
            sync_start = start_date - timedelta(days=t["start_offset"])
            sync_end = end_date + timedelta(days=t["end_offset"])
            num_parts = (sync_end - sync_start).days + 1
            rows.append({
                "table": table,
                "partition_col": t["partition_col"],
                "sync_start": str(sync_start),
                "sync_end": str(sync_end),
                "num_partitions": str(num_parts),
                "note": t.get("note", ""),
            })

    return {
        "project": project_name,
        "azkaban_project": config.get("azkaban_project", ""),
        "azkaban_flow": config.get("azkaban_flow", ""),
        "run_date": run_date_str,
        "num_days_load": ndl,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "rows": rows,
    }


def _format_plan(plan):
    """Render a sync plan as a markdown table."""
    lines = []
    lines.append(f"Project: {plan['project']} | run_date: {plan['run_date']} | num_days_load: {plan['num_days_load']}")
    lines.append(f"start_date: {plan['start_date']} | end_date: {plan['end_date']}")
    if plan.get("azkaban_project"):
        lines.append(f"Azkaban: {plan['azkaban_project']} / {plan['azkaban_flow']}")
    lines.append("")

    # Determine column widths from data
    max_table = max(len(r["table"]) for r in plan["rows"])
    max_pcol = max(len(r["partition_col"]) for r in plan["rows"])
    tw = max(max_table, 5)  # min width for "Table" header
    pw = max(max_pcol, 13)  # min width for "Partition Col" header

    header = f"| {'Table':<{tw}} | {'Partition Col':<{pw}} | {'Sync Start':<10} | {'Sync End':<10} | {'Partitions':>10} |"
    sep = f"|{'-' * (tw + 2)}|{'-' * (pw + 2)}|{'-' * 12}|{'-' * 12}|{'-' * 12}|"
    lines.append(header)
    lines.append(sep)

    for r in plan["rows"]:
        note = f" ({r['note']})" if r.get("note") else ""
        lines.append(
            f"| {r['table']:<{tw}} | {r['partition_col']:<{pw}} | {r['sync_start']:<10} | {r['sync_end']:<10} | {r['num_partitions']:>10} |{note}"
        )

    return "\n".join(lines)


def register(mcp):

    @mcp.tool()
    def calculate_sync_plan(
        project_name: str,
        run_date: str,
        num_days_load: int = 0,
    ) -> str:
        """Calculate which tables need syncing and their date ranges for a dev run.

        Returns a formatted table showing each upstream table, its partition
        column, the sync date range, and number of partitions to sync.
        Pass num_days_load=0 (default) to use the project's standard value.

        Args:
            project_name: Project name (e.g. "diner_session_topics", "clickstream_pv_metric_analysis", "diner_session_summary")
            run_date: The run.date parameter (YYYY-MM-DD)
            num_days_load: Number of days to load (0 = use project default)
        """
        if project_name not in SYNC_CONFIGS:
            available = "\n".join(f"  - {k} (default num_days_load={v['default_num_days_load']})" for k, v in sorted(SYNC_CONFIGS.items()))
            return f"Unknown project '{project_name}'.\n\nAvailable projects:\n{available}"

        try:
            plan = _calculate_plan(project_name, run_date, num_days_load)
            return _format_plan(plan)
        except ValueError as e:
            return f"Invalid run_date format: {e}. Expected YYYY-MM-DD."
