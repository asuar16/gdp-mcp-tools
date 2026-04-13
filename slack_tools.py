"""Slack tools for GDP MCP server.

Supports two auth modes:
1. User tokens (xoxc + xoxd) - posts/reads as YOUR name (default)
2. Bot token (xoxb) - posts as the bot app name (fallback)

For user tokens, extract from browser dev tools:
- SLACK_MCP_XOXC_TOKEN: from API request body (token=xoxc-...)
- SLACK_MCP_XOXD_TOKEN: from cookie header (d=xoxd-...)
- SLACK_MCP_USER_AGENT: from User-Agent request header
"""

import json
import logging
import os
import re

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


def _slack_api_get(endpoint, params, config):
    """Make a GET request to Slack API using user tokens."""
    headers = {
        "Authorization": f"Bearer {config['xoxc']}",
        "Cookie": f"d={config['xoxd']}",
    }
    if config.get("user_agent"):
        headers["User-Agent"] = config["user_agent"]
    resp = requests.get(
        f"https://slack.com/api/{endpoint}",
        headers=headers,
        params=params,
        timeout=15,
    )
    return resp.json()


def _parse_slack_url(url):
    """Parse a Slack message URL into channel_id and message_ts.

    Supports formats:
    - https://grubhub.slack.com/archives/C735U9F0C/p1775066684867499
    - https://grubhub.slack.com/archives/C735U9F0C/p1775066684867499?thread_ts=...
    """
    match = re.search(r"/archives/([A-Z0-9]+)/p(\d{10})(\d{6})", url)
    if not match:
        return None, None
    channel_id = match.group(1)
    ts = f"{match.group(2)}.{match.group(3)}"
    return channel_id, ts


def _format_message(msg):
    """Format a Slack message dict into readable text."""
    user = msg.get("user", msg.get("username", "unknown"))
    text = msg.get("text", "")
    ts = msg.get("ts", "")
    attachments = msg.get("attachments", [])
    files = msg.get("files", [])

    parts = [f"**{user}** ({ts}):", text]
    for att in attachments:
        if att.get("text"):
            parts.append(f"  [attachment] {att['text']}")
        elif att.get("fallback"):
            parts.append(f"  [attachment] {att['fallback']}")
    for f in files:
        parts.append(f"  [file] {f.get('name', 'unknown')} ({f.get('mimetype', '')})")
    return "\n".join(parts)


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

    @mcp.tool()
    def slack_read_message(
        url: str = "",
        channel: str = "",
        ts: str = "",
    ) -> str:
        """Read a specific Slack message by URL or channel+timestamp.

        Provide either a Slack message URL, or both channel and ts.

        Args:
            url: Slack message URL (e.g. https://grubhub.slack.com/archives/C735U9F0C/p1775066684867499)
            channel: Channel ID (e.g. C735U9F0C). Used if url is not provided.
            ts: Message timestamp (e.g. 1775066684.867499). Used if url is not provided.
        """
        config = _get_config()
        if not config or config["mode"] != "user":
            return json.dumps({"error": "User tokens (xoxc/xoxd) required for reading messages. Bot tokens cannot read."})

        if url:
            channel, ts = _parse_slack_url(url)
            if not channel:
                return json.dumps({"error": f"Could not parse Slack URL: {url}"})

        if not channel or not ts:
            return json.dumps({"error": "Provide either url, or both channel and ts"})

        try:
            data = _slack_api_get("conversations.history", {"channel": channel, "latest": ts, "inclusive": "true", "limit": "1"}, config)
        except Exception as e:
            return json.dumps({"error": str(e)})

        if not data.get("ok"):
            return json.dumps({"error": f"Slack API error: {data.get('error', 'unknown')}"})

        messages = data.get("messages", [])
        if not messages:
            return json.dumps({"error": "Message not found"})

        msg = messages[0]
        return json.dumps({
            "result": "OK",
            "channel": channel,
            "ts": msg.get("ts"),
            "user": msg.get("user", ""),
            "text": msg.get("text", ""),
            "thread_ts": msg.get("thread_ts", ""),
            "reply_count": msg.get("reply_count", 0),
            "attachments": msg.get("attachments", []),
            "files": [{"name": f.get("name"), "mimetype": f.get("mimetype")} for f in msg.get("files", [])],
            "formatted": _format_message(msg),
        })

    @mcp.tool()
    def slack_read_thread(
        url: str = "",
        channel: str = "",
        ts: str = "",
        limit: int = 50,
    ) -> str:
        """Read all replies in a Slack thread.

        Provide either a Slack message URL (of the parent message), or both channel and ts.

        Args:
            url: Slack message URL of the thread parent
            channel: Channel ID. Used if url is not provided.
            ts: Thread parent timestamp. Used if url is not provided.
            limit: Max replies to fetch (default 50, max 200)
        """
        config = _get_config()
        if not config or config["mode"] != "user":
            return json.dumps({"error": "User tokens (xoxc/xoxd) required for reading threads. Bot tokens cannot read."})

        if url:
            channel, ts = _parse_slack_url(url)
            if not channel:
                return json.dumps({"error": f"Could not parse Slack URL: {url}"})

        if not channel or not ts:
            return json.dumps({"error": "Provide either url, or both channel and ts"})

        limit = min(limit, 200)

        try:
            data = _slack_api_get("conversations.replies", {"channel": channel, "ts": ts, "limit": str(limit)}, config)
        except Exception as e:
            return json.dumps({"error": str(e)})

        if not data.get("ok"):
            return json.dumps({"error": f"Slack API error: {data.get('error', 'unknown')}"})

        messages = data.get("messages", [])
        formatted = [_format_message(m) for m in messages]

        return json.dumps({
            "result": "OK",
            "channel": channel,
            "thread_ts": ts,
            "message_count": len(messages),
            "messages": [{"user": m.get("user", ""), "text": m.get("text", ""), "ts": m.get("ts", "")} for m in messages],
            "formatted": "\n---\n".join(formatted),
        })

    @mcp.tool()
    def slack_search(
        query: str,
        sort: str = "timestamp",
        count: int = 10,
    ) -> str:
        """Search Slack messages.

        Args:
            query: Search query (supports Slack search syntax: in:#channel, from:@user, etc.)
            sort: Sort by 'timestamp' (newest first) or 'score' (relevance). Default: timestamp.
            count: Number of results (default 10, max 50)
        """
        config = _get_config()
        if not config or config["mode"] != "user":
            return json.dumps({"error": "User tokens (xoxc/xoxd) required for search. Bot tokens cannot search."})

        count = min(count, 50)

        try:
            data = _slack_api_get("search.messages", {"query": query, "sort": sort, "count": str(count)}, config)
        except Exception as e:
            return json.dumps({"error": str(e)})

        if not data.get("ok"):
            return json.dumps({"error": f"Slack API error: {data.get('error', 'unknown')}"})

        matches = data.get("messages", {}).get("matches", [])
        results = []
        for m in matches:
            results.append({
                "channel": m.get("channel", {}).get("name", ""),
                "user": m.get("username", ""),
                "text": m.get("text", ""),
                "ts": m.get("ts", ""),
                "permalink": m.get("permalink", ""),
            })

        return json.dumps({
            "result": "OK",
            "query": query,
            "total": data.get("messages", {}).get("total", 0),
            "returned": len(results),
            "matches": results,
        })