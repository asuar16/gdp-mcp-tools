"""Slack tools for GDP MCP server.

Supports two auth modes:
1. User tokens (xoxc + xoxd) - posts as YOUR name (default)
2. Bot token (xoxb) - posts as the bot app name (fallback)

For user tokens, extract from browser dev tools:
- SLACK_MCP_XOXC_TOKEN: from API request body (token=xoxc-...)
- SLACK_MCP_XOXD_TOKEN: from cookie header (d=xoxd-...)
- SLACK_MCP_USER_AGENT: from User-Agent request header
"""

import json
import logging
import os

import requests

logger = logging.getLogger(__name__)


def _get_config():
    """Get Slack config, preferring user tokens over bot token."""
    xoxc = os.environ.get("SLACK_MCP_XOXC_TOKEN", "")
    xoxd = os.environ.get("SLACK_MCP_XOXD_TOKEN", "")
    bot_token = os.environ.get("SLACK_TOKEN", "")
    user_agent = os.environ.get("SLACK_MCP_USER_AGENT", "")
    channel = os.environ.get("SLACK_DEFAULT_CHANNEL", "#team-data-clickstream")

    if xoxc and xoxd:
        return {
            "mode": "user",
            "xoxc": xoxc,
            "xoxd": xoxd,
            "user_agent": user_agent,
            "channel": channel,
        }
    elif bot_token:
        return {
            "mode": "bot",
            "token": bot_token,
            "channel": channel,
        }
    return None


def _post_as_user(channel, text, config):
    """Post as the authenticated user using xoxc/xoxd tokens."""
    headers = {
        "Authorization": f"Bearer {config['xoxc']}",
        "Content-Type": "application/json; charset=utf-8",
        "Cookie": f"d={config['xoxd']}",
    }
    if config.get("user_agent"):
        headers["User-Agent"] = config["user_agent"]

    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers=headers,
        json={"channel": channel, "text": text},
        timeout=15,
    )
    return resp.json()


def _post_as_bot(channel, text, config):
    """Post as the bot using xoxb token."""
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {config['token']}"},
        json={"channel": channel, "text": text},
        timeout=15,
    )
    return resp.json()


def register(mcp):

    @mcp.tool()
    def slack_post(
        message: str,
        channel: str = "",
    ) -> str:
        """Post a message to a Slack channel.

        Posts as your user account if user tokens (xoxc/xoxd) are configured,
        otherwise falls back to bot token.

        Args:
            message: Message text to post (supports Slack markdown)
            channel: Slack channel name (default: #team-data-clickstream)
        """
        config = _get_config()
        if not config:
            return json.dumps({"error": "No Slack tokens configured. Set SLACK_MCP_XOXC_TOKEN + SLACK_MCP_XOXD_TOKEN (user) or SLACK_TOKEN (bot) in .env"})

        ch = channel or config["channel"]
        mode = config["mode"]

        try:
            if mode == "user":
                data = _post_as_user(ch, message, config)
            else:
                data = _post_as_bot(ch, message, config)
        except Exception as e:
            return json.dumps({"error": str(e)})

        if not data.get("ok"):
            error = data.get("error", "unknown")
            # If user token fails, try bot fallback
            if mode == "user" and os.environ.get("SLACK_TOKEN"):
                logger.warning("User token failed (%s), falling back to bot", error)
                try:
                    bot_config = {"token": os.environ["SLACK_TOKEN"], "channel": ch}
                    data = _post_as_bot(ch, message, bot_config)
                    if data.get("ok"):
                        return json.dumps({
                            "result": "POSTED",
                            "channel": ch,
                            "mode": "bot (fallback)",
                            "ts": data.get("ts"),
                            "message": f"Message posted to {ch} (bot fallback - user token expired)",
                        })
                except Exception:
                    pass
            return json.dumps({"error": f"Slack API error: {error}"})

        return json.dumps({
            "result": "POSTED",
            "channel": ch,
            "mode": mode,
            "ts": data.get("ts"),
            "message": f"Message posted to {ch} as {'your user' if mode == 'user' else 'bot'}",
        })