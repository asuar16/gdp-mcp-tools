# GDP MCP Tools

MCP server for Grubhub Data Platform operations -- 37 tools for Jenkins, Azkaban, Trino, Jira, Slack, VPN, PV Analysis, and more.

## Quick Start

```bash
git clone https://github.com/asuar16/gdp-mcp-tools.git
cd gdp-mcp-tools
python3 -m venv mcp_venv && source mcp_venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # fill in your credentials
cp .mcp.json.example .mcp.json
# Edit config.json with your events-mart repo path (for PV tools)
```

See [TOOLS.md](TOOLS.md) for the full 37-tool reference.

---

## Setup Guide

Cross-platform setup for the Grubhub Data Platform MCP server. Works with Claude Code, GitHub Copilot, VS Code, Cursor, or any MCP-compatible client.

## Prerequisites

- Python 3.9+
- Git
- VPN access to Grubhub internal network
- Credentials: Jenkins API token, Okta password, Trino password, Jira PAT

## 1. Create Virtual Environment

```bash
# macOS / Linux
python3 -m venv mcp_venv
source mcp_venv/bin/activate

# Windows (PowerShell)
python -m venv mcp_venv
.\mcp_venv\Scripts\Activate.ps1

# Windows (CMD)
python -m venv mcp_venv
mcp_venv\Scripts\activate.bat
```

## 2. Install Dependencies

```bash
pip install -r requirements_mcp.txt
```

`requirements_mcp.txt` contents:
```
mcp[cli]>=1.2.0
requests
python-dotenv
networkx>=3.0
trino>=0.336.0
sqlglot>=27.0
pandas>=2.0
lxml>=5.0
fabric>=3.2
```

## 3. Configure Credentials

Copy the example and fill in your credentials:

```bash
cp src/mcp_servers/gdp/.env.example src/mcp_servers/gdp/.env
```

Edit `src/mcp_servers/gdp/.env`:

```env
# === Identity ===
USERNAME=your-ldap-username

# === Jenkins ===
JENKINS_TOKEN=your-jenkins-api-token

# === Azkaban + Flowlogs SSH ===
OKTA_PASSWORD=your-okta-password

# === Trino (Prod) ===
PRESTO_HOST=presto.gdp.data.grubhub.com
PRESTO_PASSWORD=your-trino-password
PRESTO_PORT=443
PRESTO_CATALOG=hive

# === Trino (Dev) ===
DEV_PRESTO_HOST=dev-presto.gdp.data.grubhub.com

# === Jira ===
JIRA_URL=https://grubhub.atlassian.net
JIRA_USERNAME=your-email@grubhub.com
JIRA_PERSONAL_TOKEN=your-jira-pat
JIRA_PROJECTS_FILTER=OED

# === GitHub (optional, for PR tools) ===
GITHUB_TOKEN=ghp_your-github-pat
```

### Where to get credentials

| Credential | How to get |
|------------|-----------|
| `JENKINS_TOKEN` | Jenkins > User Profile > Configure > API Token |
| `OKTA_PASSWORD` | Your Okta login password |
| `PRESTO_PASSWORD` | Trino cluster password (ask team lead) |
| `JIRA_PERSONAL_TOKEN` | Jira > Profile > Personal Access Tokens > Create token |
| `GITHUB_TOKEN` | GitHub > Settings > Developer Settings > Personal Access Tokens (needs `repo` + `read:org` scopes, SSO authorized for GrubhubProd) |

## 4. Configure Your MCP Client

### Claude Code

Create `.mcp.json` in the repo root:

```json
{
  "mcpServers": {
    "gdp-tools": {
      "type": "stdio",
      "command": "mcp_venv/bin/python",
      "args": ["src/mcp_servers/gdp/server.py"]
    }
  }
}
```

**Windows** -- adjust the python path:
```json
{
  "mcpServers": {
    "gdp-tools": {
      "type": "stdio",
      "command": "mcp_venv\\Scripts\\python.exe",
      "args": ["src/mcp_servers/gdp/server.py"]
    }
  }
}
```

### VS Code / Cursor (with MCP extension)

Add to `.vscode/settings.json`:

```json
{
  "mcp.servers": {
    "gdp-tools": {
      "type": "stdio",
      "command": "${workspaceFolder}/mcp_venv/bin/python",
      "args": ["${workspaceFolder}/src/mcp_servers/gdp/server.py"]
    }
  }
}
```

### GitHub Copilot (MCP endpoint)

Add to `.mcp.json`:

```json
{
  "mcpServers": {
    "gdp-tools": {
      "type": "stdio",
      "command": "mcp_venv/bin/python",
      "args": ["src/mcp_servers/gdp/server.py"]
    },
    "github": {
      "type": "http",
      "url": "https://api.githubcopilot.com/mcp/",
      "headers": {
        "Authorization": "Bearer YOUR_GITHUB_COPILOT_TOKEN"
      }
    }
  }
}
```

### Generic MCP Client (any client)

The server uses **stdio transport**. Start it with:

```bash
# macOS / Linux
./mcp_venv/bin/python src/mcp_servers/gdp/server.py

# Windows
.\mcp_venv\Scripts\python.exe src\mcp_servers\gdp\server.py
```

The server reads JSON-RPC messages from stdin and writes responses to stdout. All logs go to stderr.

## 5. Verify Setup

```bash
# Test the server starts without errors
./mcp_venv/bin/python -c "
import sys
sys.path.insert(0, 'src/mcp_servers/gdp')
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path('src/mcp_servers/gdp/.env'))
import auth
print('Username:', auth.get_username())
print('Jenkins URL:', auth.jenkins_url())
print('Azkaban URL:', auth.azkaban_url())
print('Jira URL:', auth.jira_url())
print('Setup OK')
"
```

## 6. Connect VPN

All GDP tools require VPN access. The server includes VPN management tools:

- `vpn_connect` -- opens F5 VPN browser auth
- `vpn_status` -- checks if VPN is connected
- `vpn_disconnect` -- disconnects VPN

Always run `vpn_connect` (or connect manually) before using any other tools.

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `ModuleNotFoundError: No module named 'mcp'` | `pip install mcp[cli]` in the venv |
| `PRESTO_HOST must be set` | Check `.env` file exists and has correct values |
| `Connection timed out` | Connect VPN first |
| `Azkaban login failed` | Check OKTA_PASSWORD is current |
| `Jenkins 401` | Regenerate JENKINS_TOKEN |
| `Jira 401` | Regenerate JIRA_PERSONAL_TOKEN |
| `dev-presto DNS resolution failed` | VPN may have dropped, reconnect |
| Tools not showing up | Restart your MCP client (Claude Code, VS Code, etc.) |

## File Structure

```
src/mcp_servers/gdp/
  .env              # Credentials (gitignored)
  .env.example      # Template for .env
  server.py         # MCP server entry point
  auth.py           # Shared authentication (Jenkins, Azkaban, Trino, Jira)
  jenkins_tools.py  # Table sync, deploy, validate, integrate, cluster mgmt
  scheduler_tools.py# Azkaban workflow management
  trino_tools.py    # SQL query execution
  emr_tools.py      # EMR cluster info
  pr_tools.py       # GitHub PR discussions
  flowlogs_tools.py # Azkaban log retrieval via SSH
  vpn_tools.py      # F5 VPN connect/disconnect/status
  jira_tools.py     # Jira issue management
  slack_tools.py    # Slack messaging
  spark_tools.py    # Spark History Server metrics
  sync_plan_tools.py# Smart sync planning
  pv_tools.py       # PV failure analysis, reports, root cause investigation
```
