"""Redash tools for GDP MCP server.

Create, execute, and manage Redash saved queries across dev, preprod, and prod.
"""

import json
import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

_REDASH_CONFIGS = {
    "dev": {
        "url": os.environ.get("REDASH_URL", "https://dev-redash.gdp.data.grubhub.com"),
        "api_key": os.environ.get("REDASH_API_KEY", ""),
        "default_ds": 2,
    },
    "preprod": {
        "url": os.environ.get("REDASH_URL_PREPROD", "https://preprod-redash.gdp.data.grubhub.com"),
        "api_key": os.environ.get("REDASH_API_KEY_PREPROD", ""),
        "default_ds": 1,
    },
    "prod": {
        "url": os.environ.get("REDASH_URL_PROD", "https://redash.gdp.data.grubhub.com"),
        "api_key": os.environ.get("REDASH_API_KEY_PROD", ""),
        "default_ds": 11,
    },
}


def _get_session(env):
    cfg = _REDASH_CONFIGS.get(env)
    if not cfg or not cfg["api_key"]:
        raise RuntimeError(f"Redash API key not configured for env={env}. Check .env file.")
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Key {cfg['api_key']}",
        "Content-Type": "application/json",
    })
    return session, cfg


def register(mcp):

    @mcp.tool()
    def redash_create_query(
        name: str,
        query: str,
        env: str = "prod",
        data_source_id: int = 0,
        description: str = "",
    ) -> str:
        """Create a saved query in Redash.

        Args:
            name: Query name/title
            query: SQL query string
            env: Target Redash environment - "dev", "preprod", or "prod" (default: prod)
            data_source_id: Data source ID (0 = use default for env: dev=2, preprod=1, prod=11)
            description: Optional query description
        """
        try:
            session, cfg = _get_session(env)
            ds_id = data_source_id if data_source_id > 0 else cfg["default_ds"]

            payload = {"name": name, "query": query, "data_source_id": ds_id}
            if description:
                payload["description"] = description

            resp = session.post(f"{cfg['url']}/api/queries", json=payload)
            resp.raise_for_status()
            data = resp.json()
            query_id = data["id"]
            url = f"{cfg['url']}/queries/{query_id}"

            return json.dumps({
                "result": "CREATED",
                "query_id": query_id,
                "url": url,
                "env": env,
                "data_source_id": ds_id,
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def redash_run_query(
        query_id: int,
        env: str = "prod",
        max_wait_sec: int = 120,
    ) -> str:
        """Execute a Redash query and return results.

        Triggers execution, polls for completion, and returns the result data.

        Args:
            query_id: Redash query ID to execute
            env: Target Redash environment - "dev", "preprod", or "prod" (default: prod)
            max_wait_sec: Maximum seconds to wait for results (default: 120)
        """
        try:
            session, cfg = _get_session(env)
            base = cfg["url"]

            # Get query to find data source
            q = session.get(f"{base}/api/queries/{query_id}").json()
            ds_id = q.get("data_source_id")
            query_text = q.get("query", "")

            # Trigger execution
            resp = session.post(f"{base}/api/query_results", json={
                "data_source_id": ds_id,
                "query": query_text,
                "max_age": 0,
            })

            if resp.status_code == 200:
                data = resp.json()
                if "query_result" in data:
                    rows = data["query_result"]["data"]["rows"]
                    cols = [c["name"] for c in data["query_result"]["data"]["columns"]]
                    return json.dumps({
                        "result": "SUCCESS",
                        "query_id": query_id,
                        "columns": cols,
                        "rows": rows[:500],
                        "row_count": len(rows),
                        "truncated": len(rows) > 500,
                    })
                elif "job" in data:
                    job_id = data["job"]["id"]
                    # Poll for completion
                    start = time.time()
                    while time.time() - start < max_wait_sec:
                        time.sleep(3)
                        job_resp = session.get(f"{base}/api/jobs/{job_id}").json()
                        status = job_resp.get("job", {}).get("status")
                        if status == 3:  # done
                            qr_id = job_resp["job"].get("query_result_id")
                            if qr_id:
                                result = session.get(f"{base}/api/query_results/{qr_id}").json()
                                rows = result["query_result"]["data"]["rows"]
                                cols = [c["name"] for c in result["query_result"]["data"]["columns"]]
                                return json.dumps({
                                    "result": "SUCCESS",
                                    "query_id": query_id,
                                    "columns": cols,
                                    "rows": rows[:500],
                                    "row_count": len(rows),
                                    "truncated": len(rows) > 500,
                                })
                        elif status == 4:  # error
                            error = job_resp["job"].get("error", "Unknown error")
                            return json.dumps({"error": f"Query failed: {error}"})

                    return json.dumps({"error": f"Timed out after {max_wait_sec}s. Job {job_id} still running."})

            return json.dumps({"error": f"Unexpected response: {resp.status_code} {resp.text[:200]}"})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def redash_update_query(
        query_id: int,
        query: str = "",
        name: str = "",
        env: str = "prod",
    ) -> str:
        """Update an existing Redash query.

        Args:
            query_id: Redash query ID to update
            query: New SQL query string (empty = don't change)
            name: New query name (empty = don't change)
            env: Target Redash environment - "dev", "preprod", or "prod" (default: prod)
        """
        try:
            session, cfg = _get_session(env)
            payload = {}
            if query:
                payload["query"] = query
            if name:
                payload["name"] = name
            if not payload:
                return json.dumps({"error": "Nothing to update. Provide query or name."})

            resp = session.post(f"{cfg['url']}/api/queries/{query_id}", json=payload)
            resp.raise_for_status()
            data = resp.json()
            return json.dumps({
                "result": "UPDATED",
                "query_id": data["id"],
                "url": f"{cfg['url']}/queries/{data['id']}",
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def redash_create_visualization(
        query_id: int,
        name: str,
        x_column: str,
        y_columns: str,
        chart_type: str = "line",
        env: str = "prod",
    ) -> str:
        """Create a chart visualization on a Redash query.

        Adds a line/column/bar chart tab to an existing query.
        Run the query first so the chart renders with data.

        Args:
            query_id: Redash query ID to add the visualization to
            name: Chart name (shown as tab name)
            x_column: Column name for X axis (e.g. "dt", "event_date")
            y_columns: Comma-separated column names for Y axis (e.g. "total_rows,ghost_rows")
            chart_type: Chart type - "line", "column", "bar", "area", "scatter" (default: line)
            env: Target Redash environment (default: prod)
        """
        try:
            session, cfg = _get_session(env)
            y_cols = [c.strip() for c in y_columns.split(",") if c.strip()]
            if not y_cols:
                return json.dumps({"error": "y_columns must have at least one column"})

            column_mapping = {x_column: "x"}
            series_options = {}
            for i, col in enumerate(y_cols):
                column_mapping[col] = "y"
                series_options[col] = {"zIndex": i, "index": 0, "type": chart_type, "yAxis": 0}

            payload = {
                "name": name,
                "type": "CHART",
                "query_id": query_id,
                "options": {
                    "globalSeriesType": chart_type,
                    "sortX": True,
                    "legend": {"enabled": True},
                    "xAxis": {"type": "-", "labels": {"enabled": True}},
                    "yAxis": [{"type": "linear"}],
                    "series": {"stacking": None, "error_y": {"visible": True, "type": "data"}},
                    "columnMapping": column_mapping,
                    "seriesOptions": series_options,
                    "valuesOptions": {},
                    "showDataLabels": False,
                    "numberFormat": "0,0[.]00",
                    "percentFormat": "0[.]00%",
                    "dateTimeFormat": "YYYY-MM-DD",
                    "textFormat": "",
                    "missingValuesAsZero": True,
                    "direction": {"type": "counterclockwise"},
                    "error_y": {"visible": True, "type": "data"},
                },
            }

            resp = session.post(f"{cfg['url']}/api/visualizations", json=payload)
            resp.raise_for_status()
            data = resp.json()
            viz_id = data["id"]

            return json.dumps({
                "result": "CREATED",
                "visualization_id": viz_id,
                "query_id": query_id,
                "name": name,
                "chart_type": chart_type,
                "url": f"{cfg['url']}/queries/{query_id}#{viz_id}",
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def redash_delete_visualization(
        visualization_id: int,
        env: str = "prod",
    ) -> str:
        """Delete a visualization from a Redash query.

        Args:
            visualization_id: Visualization ID to delete
            env: Target Redash environment (default: prod)
        """
        try:
            session, cfg = _get_session(env)
            resp = session.delete(f"{cfg['url']}/api/visualizations/{visualization_id}")
            resp.raise_for_status()
            return json.dumps({
                "result": "DELETED",
                "visualization_id": visualization_id,
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def redash_clone_query(
        source_query_id: int,
        source_env: str = "prod",
        target_env: str = "",
        new_name: str = "",
        copy_visualizations: bool = True,
    ) -> str:
        """Clone a Redash query (with all its visualizations) to same or different environment.

        Copies the SQL, description, data source, and optionally all chart visualizations.
        Use this to duplicate a query within the same env or replicate it across dev/preprod/prod.

        Example: Clone prod query 272533 with viz 371609 to dev:
            redash_clone_query(source_query_id=272533, source_env="prod", target_env="dev")

        Args:
            source_query_id: Query ID to clone from
            source_env: Source Redash environment (default: prod)
            target_env: Target environment (empty = same as source)
            new_name: New query name (empty = "Copy of <original name>")
            copy_visualizations: Copy all chart visualizations too (default: true)
        """
        try:
            target = target_env if target_env else source_env
            src_session, src_cfg = _get_session(source_env)
            tgt_session, tgt_cfg = _get_session(target)

            # 1. Fetch source query
            resp = src_session.get(f"{src_cfg['url']}/api/queries/{source_query_id}")
            resp.raise_for_status()
            src_query = resp.json()

            query_name = new_name if new_name else f"Copy of {src_query['name']}"
            query_sql = src_query.get("query", "")
            description = src_query.get("description", "")
            src_ds_id = src_query.get("data_source_id")

            # Map data source: if cross-env, use target default
            if source_env != target:
                ds_id = tgt_cfg["default_ds"]
            else:
                ds_id = src_ds_id

            # 2. Create new query in target
            payload = {
                "name": query_name,
                "query": query_sql,
                "data_source_id": ds_id,
            }
            if description:
                payload["description"] = description

            resp = tgt_session.post(f"{tgt_cfg['url']}/api/queries", json=payload)
            resp.raise_for_status()
            new_query = resp.json()
            new_query_id = new_query["id"]
            new_url = f"{tgt_cfg['url']}/queries/{new_query_id}"

            # 3. Copy visualizations
            viz_results = []
            if copy_visualizations:
                for viz in src_query.get("visualizations", []):
                    if viz["type"] == "TABLE":
                        continue  # skip default table viz

                    viz_payload = {
                        "name": viz["name"],
                        "type": viz["type"],
                        "query_id": new_query_id,
                        "options": viz.get("options", {}),
                    }
                    viz_resp = tgt_session.post(f"{tgt_cfg['url']}/api/visualizations", json=viz_payload)
                    if viz_resp.status_code == 200:
                        new_viz = viz_resp.json()
                        viz_results.append({
                            "source_viz_id": viz["id"],
                            "new_viz_id": new_viz["id"],
                            "name": viz["name"],
                            "type": viz["type"],
                        })

            return json.dumps({
                "result": "CLONED",
                "source": {
                    "query_id": source_query_id,
                    "env": source_env,
                    "name": src_query["name"],
                },
                "target": {
                    "query_id": new_query_id,
                    "env": target,
                    "url": new_url,
                    "name": query_name,
                    "data_source_id": ds_id,
                },
                "visualizations_copied": len(viz_results),
                "visualizations": viz_results,
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def redash_copy_visualization(
        source_query_id: int,
        target_query_id: int,
        visualization_id: int = 0,
        source_env: str = "prod",
        target_env: str = "",
    ) -> str:
        """Copy visualization(s) from one Redash query to another.

        Copies chart configuration (type, columns, options) from source to target query.
        The target query must already exist and ideally have the same columns.

        Args:
            source_query_id: Query ID to copy visualization from
            target_query_id: Query ID to copy visualization to
            visualization_id: Specific viz ID to copy (0 = copy all non-table visualizations)
            source_env: Source Redash environment (default: prod)
            target_env: Target environment (empty = same as source)
        """
        try:
            target = target_env if target_env else source_env
            src_session, src_cfg = _get_session(source_env)
            tgt_session, tgt_cfg = _get_session(target)

            # Fetch source query to get visualizations
            resp = src_session.get(f"{src_cfg['url']}/api/queries/{source_query_id}")
            resp.raise_for_status()
            src_query = resp.json()

            vizs_to_copy = []
            for viz in src_query.get("visualizations", []):
                if viz["type"] == "TABLE":
                    continue
                if visualization_id > 0 and viz["id"] != visualization_id:
                    continue
                vizs_to_copy.append(viz)

            if not vizs_to_copy:
                return json.dumps({"error": f"No matching visualizations found (viz_id={visualization_id})"})

            results = []
            for viz in vizs_to_copy:
                payload = {
                    "name": viz["name"],
                    "type": viz["type"],
                    "query_id": target_query_id,
                    "options": viz.get("options", {}),
                }
                viz_resp = tgt_session.post(f"{tgt_cfg['url']}/api/visualizations", json=payload)
                if viz_resp.status_code == 200:
                    new_viz = viz_resp.json()
                    results.append({
                        "source_viz_id": viz["id"],
                        "new_viz_id": new_viz["id"],
                        "name": viz["name"],
                        "type": viz["type"],
                        "url": f"{tgt_cfg['url']}/queries/{target_query_id}#{new_viz['id']}",
                    })
                else:
                    results.append({
                        "source_viz_id": viz["id"],
                        "name": viz["name"],
                        "error": f"{viz_resp.status_code}: {viz_resp.text[:100]}",
                    })

            return json.dumps({
                "result": "COPIED",
                "source_query_id": source_query_id,
                "target_query_id": target_query_id,
                "visualizations": results,
                "count": len([r for r in results if "new_viz_id" in r]),
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def redash_list_queries(
        search: str = "",
        env: str = "prod",
        limit: int = 10,
    ) -> str:
        """Search or list recent Redash queries.

        Args:
            search: Search term to filter queries (empty = list recent)
            env: Target Redash environment - "dev", "preprod", or "prod" (default: prod)
            limit: Maximum queries to return (default: 10)
        """
        try:
            session, cfg = _get_session(env)
            params = {"page_size": limit}
            if search:
                params["q"] = search
            resp = session.get(f"{cfg['url']}/api/queries", params=params)
            resp.raise_for_status()
            data = resp.json()
            queries = data.get("results", data) if isinstance(data, dict) else data

            result = []
            for q in queries[:limit]:
                result.append({
                    "id": q["id"],
                    "name": q["name"],
                    "url": f"{cfg['url']}/queries/{q['id']}",
                    "created_at": q.get("created_at", ""),
                    "data_source_id": q.get("data_source_id"),
                })

            return json.dumps({
                "result": "OK",
                "env": env,
                "queries": result,
                "count": len(result),
            })
        except Exception as e:
            return json.dumps({"error": str(e)})