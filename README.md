# GDP MCP Tools

MCP (Model Context Protocol) server for Grubhub Data Platform operations. 40+ tools across 18 service categories. Works with Claude Code, VS Code, Cursor, JetBrains, or any MCP-compatible client.

## Quick Start (5 minutes)

### 1. Clone and set up virtual environment

```bash
# Ask your team lead for the repo URL, or use:
git clone https://github.com/asuar16/gdp-mcp-tools.git
cd gdp-mcp-tools

# macOS / Linux
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Windows (PowerShell)
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp .env.example src/.env
```

Edit `src/.env` with your credentials. See [Credentials](#credentials) below.

### 3. Configure your MCP client

Add to `.mcp.json` in **any repo** where you want these tools:

**macOS / Linux:**
```json
{
  "mcpServers": {
    "gdp-tools": {
      "type": "stdio",
      "command": "/absolute/path/to/gdp-mcp-tools/venv/bin/python",
      "args": ["/absolute/path/to/gdp-mcp-tools/src/server.py"]
    }
  }
}
```

**Windows:**
```json
{
  "mcpServers": {
    "gdp-tools": {
      "type": "stdio",
      "command": "C:\\path\\to\\gdp-mcp-tools\\venv\\Scripts\\python.exe",
      "args": ["C:\\path\\to\\gdp-mcp-tools\\src\\server.py"]
    }
  }
}
```

### 4. Connect VPN and start using

All tools require VPN. Connect first, then use any MCP client.

---

## Using from Any Repo

The MCP server runs from its own directory. You reference it via **absolute paths** in `.mcp.json`:

```bash
# Find your absolute paths
echo "Python: $(pwd)/venv/bin/python"
echo "Server: $(pwd)/src/server.py"
```

Then in any other project's `.mcp.json`:
```json
{
  "mcpServers": {
    "gdp-tools": {
      "type": "stdio",
      "command": "/Users/yourname/gdp-mcp-tools/venv/bin/python",
      "args": ["/Users/yourname/gdp-mcp-tools/src/server.py"]
    }
  }
}
```

Works from any repo -- events-mart, ods_data, or even non-GDP projects.

---

## Client Setup

### Claude Code (CLI / Desktop / Web)

Add `.mcp.json` to your project root:
```json
{
  "mcpServers": {
    "gdp-tools": {
      "type": "stdio",
      "command": "/absolute/path/to/gdp-mcp-tools/venv/bin/python",
      "args": ["/absolute/path/to/gdp-mcp-tools/src/server.py"]
    }
  }
}
```

### VS Code / Cursor

Add to `.vscode/settings.json`:
```json
{
  "mcp.servers": {
    "gdp-tools": {
      "type": "stdio",
      "command": "/absolute/path/to/gdp-mcp-tools/venv/bin/python",
      "args": ["/absolute/path/to/gdp-mcp-tools/src/server.py"]
    }
  }
}
```

### Google Antigravity

Add to `~/.gemini/antigravity/mcp_config.json` (or project-level `.gemini/settings.json`):

**macOS / Linux:**
```json
{
  "mcpServers": {
    "gdp-tools": {
      "command": "/absolute/path/to/gdp-mcp-tools/venv/bin/python",
      "args": ["/absolute/path/to/gdp-mcp-tools/src/server.py"]
    }
  }
}
```

**Windows:**
```json
{
  "mcpServers": {
    "gdp-tools": {
      "command": "C:\\path\\to\\gdp-mcp-tools\\venv\\Scripts\\python.exe",
      "args": ["C:\\path\\to\\gdp-mcp-tools\\src\\server.py"]
    }
  }
}
```

To reload after config change: use **Manage MCP Servers > View raw config** in Antigravity UI, or restart.

### Gemini CLI

Add to `~/.gemini/settings.json` (global) or `.gemini/settings.json` (project-level):

**macOS / Linux:**
```json
{
  "mcpServers": {
    "gdp-tools": {
      "command": "/absolute/path/to/gdp-mcp-tools/venv/bin/python",
      "args": ["/absolute/path/to/gdp-mcp-tools/src/server.py"]
    }
  }
}
```

**Windows:**
```json
{
  "mcpServers": {
    "gdp-tools": {
      "command": "C:\\path\\to\\gdp-mcp-tools\\venv\\Scripts\\python.exe",
      "args": ["C:\\path\\to\\gdp-mcp-tools\\src\\server.py"]
    }
  }
}
```

To reload: run `/mcp reload` in Gemini CLI, or restart.

### JetBrains (IntelliJ / PyCharm)

Settings > Tools > MCP Servers > Add:
- Name: `gdp-tools`
- Command: `/absolute/path/to/gdp-mcp-tools/venv/bin/python`
- Args: `/absolute/path/to/gdp-mcp-tools/src/server.py`

### Any MCP Client (stdio transport)

```bash
/path/to/gdp-mcp-tools/venv/bin/python /path/to/gdp-mcp-tools/src/server.py
```

Reads JSON-RPC from stdin, writes responses to stdout, logs to stderr.

---

## Credentials

Copy `.env.example` to `src/.env` and fill in:

| Credential | Required | How to Get |
|------------|----------|-----------|
| `USERNAME` | Yes | Your LDAP username (auto-detected from $USER if not set) |
| `JENKINS_TOKEN` | Yes | Jenkins > Profile > Configure > API Token |
| `OKTA_PASSWORD` | Yes | Your Okta login password |
| `PRESTO_PASSWORD` | Yes | Trino cluster password (ask team lead) |
| `JIRA_PERSONAL_TOKEN` | Yes | Jira > Profile > Personal Access Tokens |
| `JIRA_USERNAME` | Yes | Your @grubhub.com email |
| `GITHUB_TOKEN` | Optional | GitHub > Settings > Developer Settings > PAT (repo + read:org, SSO for GrubhubProd) |
| `REDASH_API_KEY` | Optional | dev-redash.gdp.data.grubhub.com > Profile > API Key |
| `SLACK_MCP_XOXC_TOKEN` | Optional | Slack browser DevTools > Network > Copy from request |
| `DD_API_KEY` | Optional | DataDog > Org Settings > API Keys |
| `PAGERDUTY_USER_API_KEY` | Optional | PagerDuty > User Settings > API Token |
| `DATAHUB_TOKEN` | Optional | DataHub UI > Settings > Access Tokens |

---

## Tool Catalog

### Data Platform (13 tools)
| Tool | Description |
|------|-------------|
| `sync_table` | Sync prod table to dev |
| `jenkins_deploy_branch` | Deploy Git branch to dev |
| `jenkins_deploy` | Deploy version to prod |
| `jenkins_integrate` | Integrate PR |
| `jenkins_start_cluster` / `stop_cluster` | EMR cluster management |
| `jenkins_validate_schedule` | Validate schedule.json |
| `azkaban_run_flow` | Execute Azkaban flow |
| `azkaban_monitor` | Monitor execution until complete |
| `azkaban_status` | Check execution status |
| `azkaban_cancel` | Cancel running execution |
| `azkaban_flows` | List flows in project |
| `azkaban_list_executions` | List recent executions |
| `trino_query` | Execute SQL on dev/preprod/prod |

### Observability (7 tools)
| Tool | Description |
|------|-------------|
| `list_emr_clusters` / `describe_emr_cluster` | EMR cluster info |
| `spark_app_details` / `spark_stage_details` | Spark History Server |
| `get_flow_logs` | Azkaban execution logs via SSH |
| `vpn_connect` / `vpn_status` / `vpn_disconnect` | F5 VPN management |
| `s3_list_partitions` / `s3_list_batch_ids` | S3 data inspection |

### Analytics (7 tools)
| Tool | Description |
|------|-------------|
| `pv_failure_summary` | PV framework failure analysis |
| `pv_analyze_metric` | Deep-dive metric investigation |
| `pv_investigate_root_cause` | Root cause tracing |
| `pv_generate_report` | HTML report generation |
| `backfill_plan` / `backfill_fire` / `backfill_list_jobs` | Backfill management |
| `calculate_sync_plan` / `check_sync_status` | Smart sync planning |

### Collaboration (10 tools)
| Tool | Description |
|------|-------------|
| `jira_search` / `jira_get_issue` / `jira_create_issue` | Jira issues |
| `jira_transition_issue` / `jira_add_comment` | Jira workflow |
| `slack_post` / `slack_read_message` / `slack_read_thread` / `slack_search` | Slack |
| `github_list_prs` / `github_read_pr` / `github_comment_pr` / `github_update_pr` | GitHub PRs |
| `get_pr_discussions` | PR discussion threads |

### Data Catalog (7 tools)
| Tool | Description |
|------|-------------|
| `datahub_search` / `datahub_get_dataset` | DataHub metadata |
| `datahub_get_lineage` / `datahub_get_column_lineage` | DataHub lineage |
| `datahub_whoami` | DataHub auth check |
| `redash_run_query` / `redash_create_query` / `redash_update_query` | Redash queries |
| `redash_create_visualization` / `redash_clone_query` | Redash viz |
| `create_branded_google_doc` / `convert_md_to_branded_html` | Google Docs |

---

## File Structure

```
gdp-mcp-tools/
  README.md                 # This file
  TOOLS.md                  # Detailed tool reference
  requirements.txt          # Python dependencies
  .env.example              # Credential template (copy to src/.env)
  .mcp.json.example         # MCP client config template
  .gitignore

  src/
    server.py               # FastMCP entry point (stdio transport)
    auth.py                 # Shared auth (Jenkins, Azkaban, Trino, Jira)
    .env                    # Your credentials (gitignored, never committed)
    tools/
      jenkins_tools.py      # Deploy, sync, integrate, cluster ops
      scheduler_tools.py    # Azkaban flows, monitor, cancel
      trino_tools.py        # SQL queries (dev/preprod/prod)
      emr_tools.py          # EMR cluster list/describe
      s3_tools.py           # S3 partition listing
      backfill_tools.py     # Backfill planning + execution
      sync_plan_tools.py    # Smart sync planning
      spark_tools.py        # Spark History Server
      flowlogs_tools.py     # Azkaban log retrieval via SSH
      pv_tools.py           # PV failure analysis
      vpn_tools.py          # VPN connect/disconnect/status
      jira_tools.py         # Jira issue management
      slack_tools.py        # Slack messaging
      github_tools.py       # GitHub PR operations
      pr_tools.py           # PR discussion threads
      google_doc_tools.py   # Branded Google Doc creation
      datahub_tools.py      # DataHub lineage/search
      redash_tools.py       # Redash query/viz management

  scripts/                  # Utility scripts (not MCP tools)
    datadog_wrapper.sh      # DataDog MCP wrapper
    pagerduty_wrapper.sh    # PagerDuty MCP wrapper
    google_workspace_wrapper.sh
    create_redash_queries.py

  config/
    config.json             # Tool configuration
```

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `ModuleNotFoundError: No module named 'mcp'` | `pip install mcp[cli]` in the venv |
| `PRESTO_HOST must be set` | Check `src/.env` exists with correct values |
| `Connection timed out` | Connect VPN first (`vpn_connect` tool or manual) |
| `Azkaban login failed` | Check OKTA_PASSWORD is current |
| `Jenkins 401` | Regenerate JENKINS_TOKEN |
| `Jira 401` | Regenerate JIRA_PERSONAL_TOKEN |
| `DNS resolution failed` | VPN dropped, reconnect |
| Tools not showing in client | Restart MCP client. Check `.mcp.json` paths are absolute. |
| Windows path issues | Use `\\` in JSON paths, or forward slashes `/` |
