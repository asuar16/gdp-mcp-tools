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