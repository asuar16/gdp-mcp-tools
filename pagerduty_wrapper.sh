#!/bin/bash
# Wrapper script to load .env credentials and launch PagerDuty MCP
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | grep -E 'PAGERDUTY_' | xargs)
fi

exec uvx pagerduty-mcp --enable-write-tools