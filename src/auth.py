"""Shared authentication and connection management for GDP MCP server.

Provides lazy-initialized, cached sessions for:
- Jenkins (HTTP Basic Auth)
- Azkaban (login cookie, cached per env, re-auth on failure)
- Trino (DBAPI connection)
- CMDash (URL helper)
- Jira (HTTP Basic Auth with personal token)
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment-specific URLs
# ---------------------------------------------------------------------------
_JENKINS_URLS = {
    "dev": os.environ.get("DEV_JENKINS_URL", "https://dev-jenkins.gdp.data.grubhub.com"),
    "prod": os.environ.get("PROD_JENKINS_URL", "https://jenkins.gdp.data.grubhub.com"),
}

_AZKABAN_URLS = {
    "dev": os.environ.get("DEV_AZKABAN_URL", "http://dev-azkaban-dr.gdp.data.grubhub.com"),
    "prod": os.environ.get("PROD_AZKABAN_URL", "https://azkaban.gdp.data.grubhub.com"),
}

_CMDASH_URLS = {
    "dev": os.environ.get("DEV_CMDASH_URL", "https://dev-cmdash.gdp.data.grubhub.com"),
    "prod": os.environ.get("PROD_CMDASH_URL", "https://cmdash.gdp.data.grubhub.com"),
}

# Cached Azkaban sessions: {env: requests.Session}
_azkaban_sessions = {}


def get_username():
    """Get username from $USERNAME or $USER environment variable."""
    return os.environ.get("USERNAME", os.environ.get("USER", ""))


# ---------------------------------------------------------------------------
# Jenkins
# ---------------------------------------------------------------------------
def jenkins_url(env="dev"):
    """Get Jenkins base URL for the given environment."""
    custom = os.environ.get("JENKINS_URL")
    if custom:
        return custom
    return _JENKINS_URLS.get(env, _JENKINS_URLS["dev"])


def jenkins_session(env="dev"):
    """Create an authenticated Jenkins session (HTTP Basic Auth)."""
    user = get_username()
    token = os.environ.get("JENKINS_TOKEN", "")
    if not user or not token:
        raise RuntimeError(
            "USERNAME/USER and JENKINS_TOKEN must be set. "
            "Copy .env.example to .env and fill in your credentials."
        )
    session = requests.Session()
    session.auth = (user, token)
    return session


# ---------------------------------------------------------------------------
# Azkaban
# ---------------------------------------------------------------------------
def azkaban_url(env="dev"):
    """Get Azkaban base URL for the given environment."""
    return _AZKABAN_URLS.get(env, _AZKABAN_URLS["dev"])


def azkaban_session(env="dev"):
    """Get or create an authenticated Azkaban session.

    Sessions are cached per environment. Call clear_azkaban_session() to
    force re-authentication (e.g. after a 401).
    """
    if env in _azkaban_sessions:
        return _azkaban_sessions[env]

    user = get_username()
    password = os.environ.get("OKTA_PASSWORD", "")
    if not user or not password:
        raise RuntimeError(
            "USERNAME/USER and OKTA_PASSWORD must be set for Azkaban access."
        )

    base = azkaban_url(env)
    session = requests.Session()
    resp = session.post(
        base,
        data={"action": "login", "username": user, "password": password},
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") == "error":
        raise RuntimeError(f"Azkaban login failed: {data.get('error', 'unknown')}")

    session_id = data.get("session.id")
    if not session_id:
        raise RuntimeError("Azkaban login returned no session.id")

    # Attach session ID to all future requests via default params
    session.params = {"session.id": session_id}
    _azkaban_sessions[env] = session
    logger.info("Azkaban session established for env=%s", env)
    return session


def clear_azkaban_session(env="dev"):
    """Clear cached Azkaban session (forces re-login on next call)."""
    _azkaban_sessions.pop(env, None)


# ---------------------------------------------------------------------------
# CMDash (EMR dashboard)
# ---------------------------------------------------------------------------
def cmdash_url(env="dev"):
    """Get CMDash base URL for the given environment."""
    return _CMDASH_URLS.get(env, _CMDASH_URLS["dev"])


# ---------------------------------------------------------------------------
# Trino
# ---------------------------------------------------------------------------
def trino_connection(dev=True, cluster=None):
    """Create a Trino DBAPI connection.

    The ``trino`` package is imported lazily so the server can start even
    when Trino is not needed.
    """
    import trino as trino_lib

    if cluster == "preprod":
        host = os.environ.get("PRESTO_HOST_PREPROD", "")
    elif cluster == "prod":
        host = os.environ.get("PRESTO_HOST_PROD", os.environ.get("PRESTO_HOST", ""))
    elif not dev:
        host = os.environ.get("PRESTO_HOST_PROD", os.environ.get("PRESTO_HOST", ""))
    else:
        host = os.environ.get("DEV_PRESTO_HOST", os.environ.get("PRESTO_HOST", ""))
    password = os.environ.get("PRESTO_PASSWORD", "")
    port = int(os.environ.get("PRESTO_PORT", "443"))
    catalog = os.environ.get("PRESTO_CATALOG", "")
    user = get_username()

    if not host:
        raise RuntimeError("PRESTO_HOST (or DEV_PRESTO_HOST for dev) must be set.")
    if not password:
        raise RuntimeError("PRESTO_PASSWORD must be set for Trino.")

    kwargs = {
        "host": host,
        "port": port,
        "user": user,
        "http_scheme": "https",
        "auth": trino_lib.auth.BasicAuthentication(user, password),
    }
    if catalog:
        kwargs["catalog"] = catalog
        kwargs["schema"] = "default"

    return trino_lib.dbapi.connect(**kwargs)


# ---------------------------------------------------------------------------
# Jira
# ---------------------------------------------------------------------------
def jira_url():
    """Get Jira base URL."""
    return os.environ.get("JIRA_URL", "https://jira.grubhub.com").rstrip("/")


def jira_session():
    """Create an authenticated Jira session.

    Detects Atlassian Cloud vs Jira Server/DC and uses the appropriate auth:
    - Cloud (atlassian.net): HTTP Basic Auth with email + API token
    - Server/DC: Bearer token auth with personal access token
    """
    token = os.environ.get("JIRA_PERSONAL_TOKEN", "")
    if not token:
        raise RuntimeError(
            "JIRA_PERSONAL_TOKEN must be set for Jira access."
        )
    session = requests.Session()
    base = jira_url()
    if "atlassian.net" in base:
        user = os.environ.get("JIRA_USERNAME", "")
        if not user:
            raise RuntimeError(
                "JIRA_USERNAME (email) must be set for Atlassian Cloud."
            )
        session.auth = (user, token)
        session.headers.update({"Content-Type": "application/json"})
    else:
        session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
    return session


def jira_default_project():
    """Get the default Jira project filter."""
    return os.environ.get("JIRA_PROJECTS_FILTER", "OED")
