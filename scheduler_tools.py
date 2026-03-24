"""Azkaban scheduler tools for GDP MCP server.

Provides tools to run flows, check execution status, cancel executions,
monitor until completion, inspect flow DAGs, and list recent executions.
"""

import json
import logging
import time

import auth

logger = logging.getLogger(__name__)

MONITOR_POLL_INTERVAL = 30  # seconds
MONITOR_TIMEOUT = 7200  # 2 hours max


def _azkaban_request(env, method, endpoint, **kwargs):
    """Make an authenticated Azkaban request with automatic re-auth on 401."""
    session = auth.azkaban_session(env)
    base = auth.azkaban_url(env)
    url = f"{base}/{endpoint}" if endpoint else base

    resp = getattr(session, method)(url, **kwargs)

    # Re-authenticate on session expiry
    if resp.status_code == 401 or (
        resp.headers.get("Content-Type", "").startswith("application/json")
        and resp.json().get("error") == "session"
    ):
        logger.info("Azkaban session expired, re-authenticating...")
        auth.clear_azkaban_session(env)
        session = auth.azkaban_session(env)
        resp = getattr(session, method)(url, **kwargs)

    resp.raise_for_status()
    return resp


def _fetch_exec_status(env, execid):
    """Fetch execution status for a given exec ID. Returns dict."""
    resp = _azkaban_request(
        env, "get", "executor",
        params={"ajax": "fetchexecflow", "execid": execid},
    )
    return resp.json()


def _format_exec_status(data):
    """Format execution status data into a clean summary dict."""
    nodes = data.get("nodes", [])
    node_summary = []
    for node in sorted(nodes, key=lambda n: n.get("startTime", 0)):
        node_summary.append({
            "id": node.get("id"),
            "status": node.get("status"),
            "start": node.get("startTime"),
            "end": node.get("endTime"),
        })

    return {
        "execid": data.get("execid"),
        "project": data.get("projectId") or data.get("project"),
        "flow": data.get("flowId") or data.get("flow"),
        "status": data.get("status"),
        "start_time": data.get("startTime"),
        "end_time": data.get("endTime"),
        "submit_user": data.get("submitUser"),
        "nodes": node_summary,
    }


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------
def register(mcp):

    @mcp.tool()
    def azkaban_run_flow(
        project: str,
        flow: str,
        jobs: str = "",
        params: str = "",
        env: str = "dev",
    ) -> str:
        """Execute an Azkaban flow.

        Args:
            project: Azkaban project name
            flow: Flow name to execute
            jobs: Comma-separated list of specific jobs to run (optional, runs all if empty)
            params: Comma-separated key=value pairs for flow parameter overrides (optional)
            env: Target environment - "dev" or "prod" (default: dev)
        """
        data = {
            "ajax": "executeFlow",
            "project": project,
            "flow": flow,
            "concurrentOption": "concurrent",
        }

        # Disable jobs not in the specified list
        # Azkaban expects disabled as a JSON array string: '["job1", "job2"]'
        if jobs:
            job_list = [j.strip() for j in jobs.split(",") if j.strip()]
            try:
                graph_resp = _azkaban_request(
                    env, "get", "manager",
                    params={"ajax": "fetchflowgraph", "project": project, "flow": flow},
                )
                all_nodes = [n["id"] for n in graph_resp.json().get("nodes", [])]
                disabled = [node for node in all_nodes if node not in job_list]
                if disabled:
                    data["disabled"] = json.dumps(disabled)
            except Exception:
                logger.warning("Could not fetch flow graph, running with jobs param only")

        # Parse flow parameter overrides
        if params:
            for pair in params.split(","):
                pair = pair.strip()
                if "=" in pair:
                    key, value = pair.split("=", 1)
                    data[f"flowOverride[{key.strip()}]"] = value.strip()

        try:
            resp = _azkaban_request(env, "post", "executor", data=data)
            result = resp.json()
        except Exception as e:
            return json.dumps({"error": str(e)})

        if "error" in result:
            return json.dumps({"error": result["error"]})

        execid = result.get("execid")
        return json.dumps({
            "result": "SUBMITTED",
            "execid": execid,
            "project": project,
            "flow": flow,
            "message": f"Flow {flow} submitted. Use azkaban_status or azkaban_monitor with execid={execid} to track.",
        })

    @mcp.tool()
    def azkaban_status(
        execid: int,
        env: str = "dev",
    ) -> str:
        """Get the status of an Azkaban flow execution.

        Args:
            execid: Execution ID returned by azkaban_run_flow
            env: Target environment - "dev" or "prod" (default: dev)
        """
        try:
            data = _fetch_exec_status(env, execid)
        except Exception as e:
            return json.dumps({"error": str(e)})

        if "error" in data:
            return json.dumps({"error": data["error"]})

        return json.dumps(_format_exec_status(data))

    @mcp.tool()
    def azkaban_cancel(
        execid: int,
        project: str = "",
        flow: str = "",
        env: str = "dev",
    ) -> str:
        """Cancel a running Azkaban flow execution.

        Args:
            execid: Execution ID to cancel
            project: Project name (optional, for confirmation)
            flow: Flow name (optional, for confirmation)
            env: Target environment - "dev" or "prod" (default: dev)
        """
        try:
            resp = _azkaban_request(
                env, "get", "executor",
                params={"ajax": "cancelFlow", "execid": execid},
            )
            result = resp.json()
        except Exception as e:
            return json.dumps({"error": str(e)})

        if "error" in result:
            return json.dumps({"error": result["error"]})

        return json.dumps({
            "result": "CANCELLED",
            "execid": execid,
            "message": f"Execution {execid} cancel requested.",
        })

    @mcp.tool()
    def azkaban_monitor(
        execid: int,
        project: str = "",
        flow: str = "",
        no_retry: bool = False,
        env: str = "dev",
    ) -> str:
        """Monitor an Azkaban execution until it completes (blocking).

        Polls every 30 seconds. Returns the final status when the execution
        finishes or times out after 2 hours.

        Args:
            execid: Execution ID to monitor
            project: Project name (optional, for display)
            flow: Flow name (optional, for display)
            no_retry: If true, do not retry failed jobs (default: false)
            env: Target environment - "dev" or "prod" (default: dev)
        """
        terminal_statuses = {
            "SUCCEEDED", "FAILED", "KILLED", "CANCELLED",
            "SKIPPED", "FAILED_FINISHING",
        }
        deadline = time.time() + MONITOR_TIMEOUT

        while time.time() < deadline:
            try:
                data = _fetch_exec_status(env, execid)
            except Exception as e:
                return json.dumps({"error": f"Failed to fetch status: {e}"})

            status = data.get("status", "UNKNOWN")
            logger.info("Execution %s status: %s", execid, status)

            if status in terminal_statuses:
                result = _format_exec_status(data)

                # Check for failed nodes
                failed_nodes = [
                    n for n in data.get("nodes", [])
                    if n.get("status") in ("FAILED", "KILLED")
                ]

                if failed_nodes:
                    result["failed_nodes"] = [
                        {"id": n.get("id"), "status": n.get("status")}
                        for n in failed_nodes
                    ]

                return json.dumps(result)

            time.sleep(MONITOR_POLL_INTERVAL)

        return json.dumps({
            "execid": execid,
            "status": "MONITOR_TIMEOUT",
            "message": f"Monitoring timed out after {MONITOR_TIMEOUT}s. Execution may still be running.",
        })

    @mcp.tool()
    def azkaban_flows(
        project: str,
        flow: str = "",
        env: str = "dev",
    ) -> str:
        """List flows in an Azkaban project, optionally showing the DAG for a specific flow.

        When a flow name is provided, returns the topologically sorted job DAG
        with dependency information.

        Args:
            project: Azkaban project name
            flow: Specific flow name to show DAG for (optional, lists all flows if empty)
            env: Target environment - "dev" or "prod" (default: dev)
        """
        try:
            if not flow:
                # List all flows in the project
                resp = _azkaban_request(
                    env, "get", "manager",
                    params={"ajax": "fetchprojectflows", "project": project},
                )
                data = resp.json()
                if "error" in data:
                    return json.dumps({"error": data["error"]})

                flows = [f.get("flowId") for f in data.get("flows", [])]
                return json.dumps({
                    "project": project,
                    "flows": sorted(flows),
                    "count": len(flows),
                })

            # Show DAG for a specific flow
            resp = _azkaban_request(
                env, "get", "manager",
                params={"ajax": "fetchflowgraph", "project": project, "flow": flow},
            )
            data = resp.json()
            if "error" in data:
                return json.dumps({"error": data["error"]})

            nodes = data.get("nodes", [])

            # Build adjacency for topological sort using networkx
            try:
                import networkx as nx

                G = nx.DiGraph()
                for node in nodes:
                    node_id = node["id"]
                    G.add_node(node_id)
                    for dep in node.get("in", []):
                        G.add_edge(dep, node_id)

                topo_order = list(nx.topological_sort(G))
            except ImportError:
                # Fallback: simple alphabetical if networkx not available
                logger.warning("networkx not installed, using alphabetical order")
                topo_order = sorted(n["id"] for n in nodes)

            # Build output with dependency info
            dep_map = {}
            for node in nodes:
                dep_map[node["id"]] = node.get("in", [])

            dag_nodes = []
            for i, node_id in enumerate(topo_order):
                dag_nodes.append({
                    "order": i + 1,
                    "job": node_id,
                    "depends_on": dep_map.get(node_id, []),
                })

            return json.dumps({
                "project": project,
                "flow": flow,
                "total_jobs": len(dag_nodes),
                "dag": dag_nodes,
            })

        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def azkaban_list_executions(
        project: str,
        flow: str,
        limit: int = 10,
        env: str = "dev",
    ) -> str:
        """List recent executions of an Azkaban flow.

        Args:
            project: Azkaban project name
            flow: Flow name
            limit: Maximum number of executions to return (default: 10)
            env: Target environment - "dev" or "prod" (default: dev)
        """
        try:
            resp = _azkaban_request(
                env, "get", "manager",
                params={
                    "ajax": "fetchFlowExecutions",
                    "project": project,
                    "flow": flow,
                    "start": 0,
                    "length": limit,
                },
            )
            data = resp.json()
        except Exception as e:
            return json.dumps({"error": str(e)})

        if "error" in data:
            return json.dumps({"error": data["error"]})

        executions = []
        for ex in data.get("executions", []):
            executions.append({
                "execid": ex.get("execId"),
                "status": ex.get("status"),
                "submit_user": ex.get("submitUser"),
                "start_time": ex.get("startTime"),
                "end_time": ex.get("endTime"),
                "submit_time": ex.get("submitTime"),
            })

        return json.dumps({
            "project": project,
            "flow": flow,
            "total": data.get("total", len(executions)),
            "executions": executions,
        })
