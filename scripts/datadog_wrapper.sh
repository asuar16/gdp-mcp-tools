#!/bin/bash
# Wrapper script to load .env credentials and launch DataDog MCP
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | grep -E 'DD_' | xargs)
fi

exec npx -y datadog-mcp-server@latest --transport stdio