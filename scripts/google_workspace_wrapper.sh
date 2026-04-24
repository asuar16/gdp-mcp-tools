#!/bin/bash
# Wrapper script to load .env credentials and launch Google Workspace MCP
# This follows the same architecture as gdp-tools (credentials in .env, not .mcp.json)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | grep -E 'GOOGLE_OAUTH_|USER_GOOGLE_EMAIL|OAUTHLIB_' | xargs)
fi

export OAUTHLIB_INSECURE_TRANSPORT=1

exec uvx workspace-mcp@latest \
    --tools gmail drive calendar docs sheets chat forms slides search \
    --single-user \
    --transport stdio