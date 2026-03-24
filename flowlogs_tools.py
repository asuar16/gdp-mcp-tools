"""Flow log retrieval tool for GDP MCP server.

SSH into Azkaban executor machines via fabric to retrieve execution logs.
Dump-all mode only (no follow/tail - MCP returns a single response).
"""

import json
import logging

import auth

logger = logging.getLogger(__name__)

# Azkaban executor log paths (common GDP conventions)
_LOG_BASE_PATHS = [
    "/var/azkaban/logs",
    "/tmp/azkaban/logs",
]


def _build_hostname(cluster, env):
    """Build the SSH hostname for the given cluster and environment."""
    if "." in cluster:
        # Already a FQDN
        return cluster
    if env == "prod":
        return f"{cluster}.gdp.data.grubhub.com"
    return f"dev-{cluster}.gdp.data.grubhub.com"


def register(mcp):

    @mcp.tool()
    def get_flow_logs(
        cluster: str,
        execid: int,
        env: str = "dev",
        user: str = "",
    ) -> str:
        """Retrieve Azkaban flow execution logs from a cluster machine via SSH.

        Connects to the executor machine and dumps all log files for the given
        execution ID. Requires SSH key-based access to the cluster.

        Args:
            cluster: Cluster hostname or short name (e.g. "azkaban-exec-1")
            execid: Azkaban execution ID
            env: Target environment - "dev" or "prod" (default: dev)
            user: SSH username (default: auto-detected from $USER)
        """
        try:
            from fabric import Connection
        except ImportError:
            return json.dumps({
                "error": "fabric is not installed. Run: pip install fabric"
            })

        hostname = _build_hostname(cluster, env)
        ssh_user = user or auth.get_username()

        if not ssh_user:
            return json.dumps({"error": "SSH user could not be determined. Set USERNAME or pass user param."})

        try:
            conn = Connection(host=hostname, user=ssh_user)
        except Exception as e:
            return json.dumps({"error": f"Failed to create SSH connection: {e}"})

        logs_found = {}
        errors = []

        for base_path in _LOG_BASE_PATHS:
            log_dir = f"{base_path}/{execid}"
            try:
                # Check if directory exists
                result = conn.run(f"test -d {log_dir} && echo exists", hide=True, warn=True)
                if result.ok and "exists" in result.stdout:
                    # List log files
                    ls_result = conn.run(f"ls -1 {log_dir}/*.log 2>/dev/null || ls -1 {log_dir}/* 2>/dev/null", hide=True, warn=True)
                    if ls_result.ok and ls_result.stdout.strip():
                        for log_file in ls_result.stdout.strip().split("\n"):
                            log_file = log_file.strip()
                            if not log_file:
                                continue
                            try:
                                cat_result = conn.run(f"cat {log_file}", hide=True, warn=True)
                                if cat_result.ok:
                                    content = cat_result.stdout
                                    # Cap individual file size
                                    if len(content) > 20000:
                                        content = "... (truncated, showing last 20000 chars) ...\n" + content[-20000:]
                                    logs_found[log_file] = content
                            except Exception as e:
                                errors.append(f"Failed to read {log_file}: {e}")
            except Exception as e:
                errors.append(f"Failed to check {log_dir}: {e}")

        try:
            conn.close()
        except Exception:
            pass

        if not logs_found and not errors:
            return json.dumps({
                "error": f"No logs found for execid={execid} on {hostname}",
                "searched_paths": [f"{p}/{execid}" for p in _LOG_BASE_PATHS],
            })

        result = {
            "cluster": hostname,
            "execid": execid,
            "log_files": list(logs_found.keys()),
            "logs": logs_found,
        }
        if errors:
            result["warnings"] = errors

        output = json.dumps(result)

        # Cap total response
        if len(output) > 50000:
            # Truncate the largest log files
            for key in sorted(logs_found, key=lambda k: len(logs_found[k]), reverse=True):
                logs_found[key] = logs_found[key][:5000] + "\n... (truncated)"
                result["logs"] = logs_found
                output = json.dumps(result)
                if len(output) <= 50000:
                    break

        return output
