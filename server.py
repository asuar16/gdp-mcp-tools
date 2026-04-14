"""GDP Tools - Unified MCP server for Grubhub Data Platform operations.

Provides 33+ tools across 10 service categories:
- Jenkins (10): table sync, deploy, validate, integrate, cluster management
- Azkaban (6): run flows, monitor, cancel, inspect DAGs, list executions
- Trino (1): SQL query execution with optional dialect transpilation
- EMR (2): list and describe clusters via CMDash
- PR (1): GitHub PR discussion threads via gh CLI
- Flowlogs (1): Azkaban execution log retrieval via SSH
- VPN (3): connect, disconnect, and status via F5 VPN app
- Jira (5): search, view, create, transition, and comment on issues
- Slack (1): post messages to channels
- PV Analysis (3): failure summary, metric deep-dive, HTML report generation
"""

import logging
import sys
from pathlib import Path

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Load .env from the same directory as this script
_env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=_env_path)

# Ensure this package directory is importable for sibling module imports
sys.path.insert(0, str(Path(__file__).resolve().parent))

# All logging goes to stderr (stdout is reserved for MCP JSON-RPC)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)

mcp = FastMCP(
    "gdp-tools",
    instructions=(
        "Unified GDP toolset for Grubhub Data Platform operations. "
        "Jenkins: sync prod tables to dev, deploy branches, validate schedules, integrate PRs, manage clusters. "
        "Azkaban: run flows, monitor executions, inspect DAGs, cancel jobs. "
        "Trino: execute SQL queries against prod/dev warehouses. "
        "EMR: list and describe clusters. "
        "PR: fetch GitHub PR discussion threads. "
        "Flowlogs: retrieve Azkaban execution logs from cluster machines. "
        "VPN: connect, disconnect, and check status of F5 VPN. "
        "Jira: search, view, create, transition, and comment on issues. "
        "Slack: post messages to channels."
    ),
)

# Import and register all tool modules
# Each module defines a register(mcp) function that uses @mcp.tool()
import jenkins_tools  # noqa: E402
import scheduler_tools  # noqa: E402
import trino_tools  # noqa: E402
import emr_tools  # noqa: E402
import pr_tools  # noqa: E402
import flowlogs_tools  # noqa: E402
import vpn_tools  # noqa: E402
import jira_tools  # noqa: E402
import slack_tools  # noqa: E402
import spark_tools  # noqa: E402
import sync_plan_tools  # noqa: E402
import pv_tools  # noqa: E402
import redash_tools  # noqa: E402
import github_tools  # noqa: E402
import s3_tools  # noqa: E402
import backfill_tools  # noqa: E402

jenkins_tools.register(mcp)
scheduler_tools.register(mcp)
trino_tools.register(mcp)
emr_tools.register(mcp)
pr_tools.register(mcp)
flowlogs_tools.register(mcp)
vpn_tools.register(mcp)
jira_tools.register(mcp)
slack_tools.register(mcp)
spark_tools.register(mcp)
sync_plan_tools.register(mcp)
pv_tools.register(mcp)
redash_tools.register(mcp)
github_tools.register(mcp)
s3_tools.register(mcp)
backfill_tools.register(mcp)

if __name__ == "__main__":
    mcp.run(transport="stdio")
