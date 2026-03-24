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
pip install -r requirements.txt
```

`requirements.txt` contents:
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
cp ./.env.example ./.env
```

Edit `./.env`:

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
      "args": ["server.py"]
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
      "args": ["server.py"]
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
      "args": ["${workspaceFolder}/server.py"]
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
      "args": ["server.py"]
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
./mcp_venv/bin/python server.py

# Windows
.\mcp_venv\Scripts\python.exe server.py
```

The server reads JSON-RPC messages from stdin and writes responses to stdout. All logs go to stderr.

## 5. Verify Setup

```bash
# Test the server starts without errors
./mcp_venv/bin/python -c "
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path('.env'))
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
gdp-mcp-tools/
  README.md           # This setup guide
  TOOLS.md            # Full 37-tool reference
  requirements.txt    # Python dependencies
  config.json         # Points to events-mart repo (for PV tools)
  .env.example        # Credential template
  .mcp.json.example   # MCP client config template
  .gitignore          # Ignores .env, __pycache__, venv
  server.py           # MCP server entry point
  auth.py             # Shared authentication (Jenkins, Azkaban, Trino, Jira)
  jenkins_tools.py    # Table sync, deploy, validate, integrate, cluster mgmt (11 tools)
  scheduler_tools.py  # Azkaban workflow management (6 tools)
  trino_tools.py      # SQL query execution (1 tool)
  emr_tools.py        # EMR cluster info (2 tools)
  pr_tools.py         # GitHub PR discussions (1 tool)
  flowlogs_tools.py   # Azkaban log retrieval via SSH (1 tool)
  vpn_tools.py        # VPN connect/disconnect/status (3 tools)
  jira_tools.py       # Jira issue management (5 tools)
  slack_tools.py      # Slack messaging (1 tool)
  spark_tools.py      # Spark History Server metrics (2 tools)
  sync_plan_tools.py  # Smart sync planning (2 tools)
  pv_tools.py         # PV failure analysis, reports, root cause investigation (4 tools)
```
