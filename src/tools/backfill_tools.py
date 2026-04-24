"""Backfill orchestrator for GDP MCP server.

Config-driven backfill tool that validates params per project before firing.
Prevents the wrong-params problem by looking up the correct Azkaban param names.
"""

import json
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Registry of backfill configs per project.
# Each entry maps a short name to the Azkaban project, flow, and param mapping.
# "date_params" defines how to map a target date to Azkaban flow params.
# "end_date_offset" handles edge cases like Ad Imp where end_date must be +1.
BACKFILL_CONFIGS = {
    "dst": {
        "display_name": "DST (diner_session_topics)",
        "azkaban_project": "events-mart_diner_session_topics_loader",
        "flow": "diner_session_topics_cleanup_backfill",
        "date_params": {
            "run.date": "{date}",
            "backfill.end.date": "{date}",
        },
        "end_date_offset": 0,
        "notes": "Runs load_topics_data_backfill + load_topics_impression_data_backfill",
    },
    "dsd": {
        "display_name": "DSD (diner_session_discovery)",
        "azkaban_project": "events-mart_diner_session_discovery_loader",
        "flow": "diner_session_discovery_cleanup_backfill",
        "date_params": {
            "run.date": "{date}",
            "start.date": "{date}",
            "end.date": "{date}",
        },
        "end_date_offset": 0,
        "notes": "Target: integrated_metrics.diner_session_discovery",
    },
    "dscsi": {
        "display_name": "DSCSI (diner_cross_sell_impressions)",
        "azkaban_project": "events-mart_diner_session_cross_sell_impressions",
        "flow": "diner_session_cross_sell_impressions_backfill",
        "date_params": {
            "run.date": "{date}",
            "backfill.start.date": "{date}",
            "backfill.end.date": "{date}",
        },
        "end_date_offset": 0,
        "notes": "python_submit.py uses range(days+1) so same-day works",
    },
    "ad_imp": {
        "display_name": "Ad Impressions (diner_ad_impressions)",
        "azkaban_project": "events-mart_diner_ad_impressions",
        "flow": "diner_ad_impressions_backfill",
        "date_params": {
            "backfill.start.date": "{date}",
            "backfill.end.date": "{end_date}",
        },
        "end_date_offset": 1,
        "notes": "end_date MUST be +1 day. python_submit.py uses range(0, days, 3) which is empty when days=0. Each run processes a 3-day window (target-2 to target).",
    },
    "dsis": {
        "display_name": "DSIS (diner_search_impression_summary)",
        "azkaban_project": "events-mart_diner_search_impression_summary_loader",
        "flow": "diner_search_impression_summary_cleanup_backfill",
        "date_params": {
            "run.date_from": "{date}",
            "run.date_to": "{date}",
        },
        "end_date_offset": 0,
        "notes": "Standard date range backfill",
    },
}


def _format_params(config, target_date_str):
    """Build Azkaban params string for a target date."""
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    end_date = target_date + timedelta(days=config["end_date_offset"])
    end_date_str = end_date.strftime("%Y-%m-%d")

    params = []
    for param_name, template in config["date_params"].items():
        value = template.replace("{date}", target_date_str).replace("{end_date}", end_date_str)
        params.append(f"{param_name}={value}")

    return ",".join(params)


def register(mcp):

    @mcp.tool()
    def backfill_plan(
        jobs: str,
        dates: str,
    ) -> str:
        """Generate a backfill plan showing exact Azkaban params for each job+date.

        Does NOT fire anything. Shows what would be fired so you can verify before running.

        Args:
            jobs: Comma-separated job short names (e.g. "dst,dsd,dscsi,ad_imp") or "all" for all known jobs
            dates: Comma-separated dates (e.g. "2026-03-15,2026-03-21,2026-04-03")
        """
        if jobs.strip().lower() == "all":
            job_list = list(BACKFILL_CONFIGS.keys())
        else:
            job_list = [j.strip() for j in jobs.split(",") if j.strip()]

        date_list = [d.strip() for d in dates.split(",") if d.strip()]

        # Validate
        errors = []
        for j in job_list:
            if j not in BACKFILL_CONFIGS:
                errors.append(f"Unknown job: {j}. Available: {', '.join(BACKFILL_CONFIGS.keys())}")
        for d in date_list:
            try:
                datetime.strptime(d, "%Y-%m-%d")
            except ValueError:
                errors.append(f"Invalid date: {d}. Use YYYY-MM-DD format.")

        if errors:
            return json.dumps({"error": errors})

        plan = []
        for j in job_list:
            cfg = BACKFILL_CONFIGS[j]
            for d in date_list:
                params = _format_params(cfg, d)
                plan.append({
                    "job": cfg["display_name"],
                    "job_key": j,
                    "date": d,
                    "azkaban_project": cfg["azkaban_project"],
                    "flow": cfg["flow"],
                    "params": params,
                    "notes": cfg["notes"],
                })

        return json.dumps({
            "result": "PLAN",
            "total_runs": len(plan),
            "jobs": len(job_list),
            "dates": len(date_list),
            "plan": plan,
            "reminder": "Override failure emails to empty in Notification tab. Disable terminate+cleanup to reuse cluster.",
        })

    @mcp.tool()
    def backfill_fire(
        jobs: str,
        dates: str,
        env: str = "prod",
        dry_run: bool = True,
    ) -> str:
        """Fire backfill runs with validated params.

        Generates the plan and optionally fires each run via azkaban_run_flow.
        Set dry_run=false to actually fire (default is dry_run=true for safety).

        Args:
            jobs: Comma-separated job short names (e.g. "dst,dsd") or "all"
            dates: Comma-separated dates (e.g. "2026-03-15,2026-03-21")
            env: Azkaban environment - "dev" or "prod" (default: prod)
            dry_run: If true, only show plan without firing (default: true)
        """
        # Import here to avoid circular dependency
        from scheduler_tools import _azkaban_run_flow_impl

        if jobs.strip().lower() == "all":
            job_list = list(BACKFILL_CONFIGS.keys())
        else:
            job_list = [j.strip() for j in jobs.split(",") if j.strip()]

        date_list = [d.strip() for d in dates.split(",") if d.strip()]

        # Validate
        for j in job_list:
            if j not in BACKFILL_CONFIGS:
                return json.dumps({"error": f"Unknown job: {j}. Available: {', '.join(BACKFILL_CONFIGS.keys())}"})
        for d in date_list:
            try:
                datetime.strptime(d, "%Y-%m-%d")
            except ValueError:
                return json.dumps({"error": f"Invalid date: {d}. Use YYYY-MM-DD format."})

        results = []
        for j in job_list:
            cfg = BACKFILL_CONFIGS[j]
            for d in date_list:
                params = _format_params(cfg, d)
                entry = {
                    "job": cfg["display_name"],
                    "date": d,
                    "project": cfg["azkaban_project"],
                    "flow": cfg["flow"],
                    "params": params,
                }

                if dry_run:
                    entry["status"] = "DRY_RUN (not fired)"
                else:
                    try:
                        # Fire via Azkaban API
                        import auth as auth_module
                        session = auth_module.azkaban_session(env)
                        base = auth_module.azkaban_url(env)

                        data = {"ajax": "executeFlow", "project": cfg["azkaban_project"], "flow": cfg["flow"]}

                        # Parse params into flowOverride format
                        for p in params.split(","):
                            key, val = p.split("=", 1)
                            data[f"flowOverride[{key}]"] = val

                        resp = session.post(f"{base}/executor", data=data)
                        resp.raise_for_status()
                        result_data = resp.json()

                        if "execid" in result_data:
                            entry["status"] = "SUBMITTED"
                            entry["execid"] = result_data["execid"]
                            entry["url"] = f"{base}/executor?execid={result_data['execid']}"
                        else:
                            entry["status"] = "ERROR"
                            entry["error"] = result_data.get("error", str(result_data))
                    except Exception as e:
                        entry["status"] = "ERROR"
                        entry["error"] = str(e)

                results.append(entry)

        return json.dumps({
            "result": "DRY_RUN" if dry_run else "FIRED",
            "total_runs": len(results),
            "env": env,
            "results": results,
            "reminder": "Override failure emails to empty in Notification tab!" if not dry_run else "",
        })

    @mcp.tool()
    def backfill_list_jobs() -> str:
        """List all known backfill job configs with their param mappings."""
        jobs = []
        for key, cfg in BACKFILL_CONFIGS.items():
            jobs.append({
                "key": key,
                "display_name": cfg["display_name"],
                "azkaban_project": cfg["azkaban_project"],
                "flow": cfg["flow"],
                "date_params": cfg["date_params"],
                "end_date_offset": cfg["end_date_offset"],
                "notes": cfg["notes"],
            })
        return json.dumps({"result": "OK", "jobs": jobs, "count": len(jobs)})