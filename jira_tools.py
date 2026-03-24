"""Jira tools for GDP MCP server.

Provides tools for searching, viewing, creating, transitioning, and
commenting on Jira issues. Uses the Jira REST API with HTTP Basic Auth
(username + personal access token).

Note: Atlassian Cloud deprecated /rest/api/2/search (returns 410 Gone).
Search uses /rest/api/3/search/jql (GET). Other endpoints still use v2.
"""

import json
import logging

import auth

logger = logging.getLogger(__name__)

API_V2 = "/rest/api/2"
API_V3 = "/rest/api/3"


def _api(method, path, api_version=2, **kwargs):
    """Make an authenticated Jira REST API call."""
    session = auth.jira_session()
    base = API_V3 if api_version == 3 else API_V2
    url = f"{auth.jira_url()}{base}{path}"
    resp = getattr(session, method)(url, timeout=30, **kwargs)
    resp.raise_for_status()
    if resp.status_code == 204 or not resp.text.strip():
        return {}
    return resp.json()


def _format_issue(issue):
    """Format a Jira issue into a concise summary dict."""
    fields = issue.get("fields", {})
    return {
        "key": issue.get("key"),
        "summary": fields.get("summary"),
        "status": (fields.get("status") or {}).get("name"),
        "type": (fields.get("issuetype") or {}).get("name"),
        "priority": (fields.get("priority") or {}).get("name"),
        "assignee": (fields.get("assignee") or {}).get("displayName"),
        "reporter": (fields.get("reporter") or {}).get("displayName"),
        "updated": fields.get("updated"),
        "created": fields.get("created"),
    }


def _format_issue_detail(issue):
    """Format a Jira issue with full details."""
    fields = issue.get("fields", {})
    result = _format_issue(issue)
    result["description"] = fields.get("description", "")
    result["labels"] = fields.get("labels", [])
    result["components"] = [c.get("name") for c in (fields.get("components") or [])]
    result["fix_versions"] = [v.get("name") for v in (fields.get("fixVersions") or [])]

    # Epic link
    epic_link = fields.get("customfield_10008") or fields.get("epic", {})
    if isinstance(epic_link, str):
        result["epic"] = epic_link
    elif isinstance(epic_link, dict):
        result["epic"] = epic_link.get("key") or epic_link.get("name")

    # Comments (last 5)
    comments = fields.get("comment", {}).get("comments", [])
    result["comments"] = [
        {
            "author": (c.get("author") or {}).get("displayName"),
            "body": c.get("body", "")[:500],
            "created": c.get("created"),
        }
        for c in comments[-5:]
    ]

    # Subtasks
    subtasks = fields.get("subtasks", [])
    if subtasks:
        result["subtasks"] = [
            {
                "key": s.get("key"),
                "summary": (s.get("fields") or {}).get("summary"),
                "status": ((s.get("fields") or {}).get("status") or {}).get("name"),
            }
            for s in subtasks
        ]

    # Links
    links = fields.get("issuelinks", [])
    if links:
        result["links"] = []
        for link in links:
            link_type = (link.get("type") or {}).get("name", "")
            if link.get("outwardIssue"):
                linked = link["outwardIssue"]
                result["links"].append({
                    "type": f"{link_type} (outward)",
                    "key": linked.get("key"),
                    "summary": (linked.get("fields") or {}).get("summary"),
                })
            if link.get("inwardIssue"):
                linked = link["inwardIssue"]
                result["links"].append({
                    "type": f"{link_type} (inward)",
                    "key": linked.get("key"),
                    "summary": (linked.get("fields") or {}).get("summary"),
                })

    return result


def register(mcp):

    @mcp.tool()
    def jira_search(
        jql: str = "",
        project: str = "",
        assignee: str = "",
        status: str = "",
        max_results: int = 20,
    ) -> str:
        """Search Jira issues using JQL or simple filters.

        Provide either a raw JQL string, or use the convenience filters
        (project, assignee, status) which get combined into JQL automatically.

        Args:
            jql: Raw JQL query (overrides other filters if provided)
            project: Filter by project key (default: from JIRA_PROJECTS_FILTER env var)
            assignee: Filter by assignee username (use "currentUser()" for yourself)
            status: Filter by status name (e.g. "In Progress", "Open")
            max_results: Maximum results to return (default: 20, max: 50)
        """
        if not jql:
            clauses = []
            proj = project or auth.jira_default_project()
            if proj:
                clauses.append(f"project = {proj}")
            if assignee:
                clauses.append(f"assignee = {assignee}")
            if status:
                clauses.append(f'status = "{status}"')
            jql = " AND ".join(clauses) + " ORDER BY updated DESC" if clauses else "ORDER BY updated DESC"

        max_results = min(max_results, 50)

        try:
            # Atlassian Cloud deprecated POST /rest/api/2/search (410 Gone).
            # Use GET /rest/api/3/search/jql instead.
            from urllib.parse import quote
            data = _api("get", f"/search/jql?jql={quote(jql)}&maxResults={max_results}"
                         f"&fields=summary,status,issuetype,priority,assignee,reporter,updated,created",
                         api_version=3)
        except Exception as e:
            return json.dumps({"error": str(e)})

        issues = [_format_issue(i) for i in data.get("issues", [])]
        return json.dumps({
            "jql": jql,
            "total": data.get("total", 0),
            "returned": len(issues),
            "issues": issues,
        })

    @mcp.tool()
    def jira_get_issue(issue_key: str) -> str:
        """Get detailed information about a Jira issue.

        Args:
            issue_key: Jira issue key (e.g. "OED-1234")
        """
        try:
            data = _api("get", f"/issue/{issue_key}")
        except Exception as e:
            return json.dumps({"error": str(e)})

        return json.dumps(_format_issue_detail(data))

    @mcp.tool()
    def jira_create_issue(
        summary: str,
        project: str = "",
        issue_type: str = "Task",
        description: str = "",
        priority: str = "",
        assignee: str = "",
        labels: str = "",
    ) -> str:
        """Create a new Jira issue.

        Args:
            summary: Issue title/summary (required)
            project: Project key (default: from JIRA_PROJECTS_FILTER env var)
            issue_type: Issue type - "Task", "Bug", "Story", "Epic" (default: Task)
            description: Issue description
            priority: Priority - "Highest", "High", "Medium", "Low", "Lowest"
            assignee: Assignee username
            labels: Comma-separated labels (e.g. "backend,bug-fix")
        """
        proj = project or auth.jira_default_project()
        if not proj:
            return json.dumps({"error": "Project is required. Set JIRA_PROJECTS_FILTER or pass project param."})

        fields = {
            "project": {"key": proj},
            "summary": summary,
            "issuetype": {"name": issue_type},
        }

        if description:
            fields["description"] = description
        if priority:
            fields["priority"] = {"name": priority}
        if assignee:
            fields["assignee"] = {"name": assignee}
        if labels:
            fields["labels"] = [l.strip() for l in labels.split(",") if l.strip()]

        try:
            data = _api("post", "/issue", json={"fields": fields})
        except Exception as e:
            return json.dumps({"error": str(e)})

        return json.dumps({
            "result": "CREATED",
            "key": data.get("key"),
            "id": data.get("id"),
            "url": f"{auth.jira_url()}/browse/{data.get('key')}",
        })

    @mcp.tool()
    def jira_transition_issue(
        issue_key: str,
        transition: str,
    ) -> str:
        """Change the status of a Jira issue (transition it).

        Args:
            issue_key: Jira issue key (e.g. "OED-1234")
            transition: Target status name (e.g. "In Progress", "Done", "To Do")
        """
        # First, get available transitions
        try:
            data = _api("get", f"/issue/{issue_key}/transitions")
        except Exception as e:
            return json.dumps({"error": str(e)})

        transitions = data.get("transitions", [])
        available = {t["name"].lower(): t for t in transitions}

        # Find matching transition (case-insensitive)
        match = available.get(transition.lower())
        if not match:
            return json.dumps({
                "error": f"Transition '{transition}' not available.",
                "available_transitions": [t["name"] for t in transitions],
            })

        # Execute transition
        try:
            _api("post", f"/issue/{issue_key}/transitions", json={
                "transition": {"id": match["id"]},
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

        return json.dumps({
            "result": "TRANSITIONED",
            "issue_key": issue_key,
            "new_status": match["name"],
        })

    @mcp.tool()
    def jira_add_comment(
        issue_key: str,
        body: str,
    ) -> str:
        """Add a comment to a Jira issue.

        Args:
            issue_key: Jira issue key (e.g. "OED-1234")
            body: Comment text
        """
        try:
            data = _api("post", f"/issue/{issue_key}/comment", json={"body": body})
        except Exception as e:
            return json.dumps({"error": str(e)})

        return json.dumps({
            "result": "COMMENT_ADDED",
            "issue_key": issue_key,
            "comment_id": data.get("id"),
            "author": (data.get("author") or {}).get("displayName"),
        })
