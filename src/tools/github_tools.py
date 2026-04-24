"""GitHub tools for GDP MCP server.

Uses GITHUB_TOKEN from env for authenticated API access.
Replaces gh CLI dependency for PR operations.
"""

import json
import logging
import os

import requests

logger = logging.getLogger(__name__)

_GITHUB_API = "https://api.github.com"


def _github_session():
    token = os.environ.get("GITHUB_TOKEN", "")
    if not token:
        raise RuntimeError("GITHUB_TOKEN must be set in .env")
    session = requests.Session()
    session.headers.update({
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    })
    return session


def _paginate(session, url, params=None, max_pages=5):
    results = []
    for _ in range(max_pages):
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            results.extend(data)
        else:
            return data
        url = resp.links.get("next", {}).get("url")
        if not url:
            break
        params = None
    return results


def register(mcp):

    @mcp.tool()
    def github_list_prs(
        author: str = "",
        state: str = "open",
        owner: str = "GrubhubProd",
        repo: str = "events-mart",
        limit: int = 10,
    ) -> str:
        """List pull requests from a GitHub repo.

        Args:
            author: Filter by PR author login (empty = all)
            state: PR state - "open", "closed", or "all" (default: open)
            owner: Repository owner (default: GrubhubProd)
            repo: Repository name (default: events-mart)
            limit: Max PRs to return (default: 10)
        """
        try:
            session = _github_session()
            params = {"state": state, "sort": "created", "direction": "desc", "per_page": min(limit * 2, 100)}
            prs = _paginate(session, f"{_GITHUB_API}/repos/{owner}/{repo}/pulls", params)

            if author:
                prs = [p for p in prs if p["user"]["login"] == author]

            result = []
            for pr in prs[:limit]:
                result.append({
                    "number": pr["number"],
                    "title": pr["title"],
                    "state": pr["state"],
                    "author": pr["user"]["login"],
                    "branch": pr["head"]["ref"],
                    "base": pr["base"]["ref"],
                    "created_at": pr["created_at"],
                    "updated_at": pr["updated_at"],
                    "mergeable": pr.get("mergeable"),
                    "changed_files": pr.get("changed_files"),
                    "additions": pr.get("additions"),
                    "deletions": pr.get("deletions"),
                    "url": pr["html_url"],
                })

            return json.dumps({"result": "OK", "count": len(result), "prs": result})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def github_read_pr(
        pr_number: int,
        owner: str = "GrubhubProd",
        repo: str = "events-mart",
        include_comments: bool = True,
        include_reviews: bool = True,
        filter_bots: bool = True,
    ) -> str:
        """Read a PR's details, comments, and review status.

        Args:
            pr_number: Pull request number
            owner: Repository owner (default: GrubhubProd)
            repo: Repository name (default: events-mart)
            include_comments: Include issue + review comments (default: true)
            include_reviews: Include review approvals/changes (default: true)
            filter_bots: Filter out bot comments (default: true)
        """
        bot_users = {"github-actions[bot]", "codecov[bot]", "dependabot[bot]", "sonarcloud[bot]", "svc-github-gdp"}

        try:
            session = _github_session()
            base = f"{_GITHUB_API}/repos/{owner}/{repo}"

            pr = session.get(f"{base}/pulls/{pr_number}", timeout=30).json()

            result = {
                "number": pr["number"],
                "title": pr["title"],
                "state": pr["state"],
                "author": pr["user"]["login"],
                "branch": pr["head"]["ref"],
                "body": pr.get("body", ""),
                "created_at": pr["created_at"],
                "changed_files": pr.get("changed_files"),
                "additions": pr.get("additions"),
                "deletions": pr.get("deletions"),
                "mergeable": pr.get("mergeable"),
                "url": pr["html_url"],
            }

            if include_comments:
                issue_comments = _paginate(session, f"{base}/issues/{pr_number}/comments")
                review_comments = _paginate(session, f"{base}/pulls/{pr_number}/comments")

                if filter_bots:
                    issue_comments = [c for c in issue_comments if c["user"]["login"] not in bot_users]
                    review_comments = [c for c in review_comments if c["user"]["login"] not in bot_users]

                result["issue_comments"] = [{
                    "author": c["user"]["login"],
                    "body": c["body"],
                    "created_at": c["created_at"],
                } for c in issue_comments]

                result["review_comments"] = [{
                    "author": c["user"]["login"],
                    "body": c["body"],
                    "path": c.get("path"),
                    "line": c.get("line"),
                    "created_at": c["created_at"],
                } for c in review_comments]

            if include_reviews:
                reviews = _paginate(session, f"{base}/pulls/{pr_number}/reviews")
                if filter_bots:
                    reviews = [r for r in reviews if r["user"]["login"] not in bot_users]
                result["reviews"] = [{
                    "author": r["user"]["login"],
                    "state": r["state"],
                    "body": r.get("body", ""),
                    "submitted_at": r.get("submitted_at"),
                } for r in reviews if r.get("state") in ("APPROVED", "CHANGES_REQUESTED", "COMMENTED")]

            output = json.dumps(result, default=str)
            if len(output) > 50000:
                result["review_comments"] = result.get("review_comments", [])[:10]
                result["issue_comments"] = result.get("issue_comments", [])[:10]
                result["note"] = "Truncated. Use filter_bots=true or check PR directly."
                output = json.dumps(result, default=str)

            return output
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def github_comment_pr(
        pr_number: int,
        body: str,
        owner: str = "GrubhubProd",
        repo: str = "events-mart",
    ) -> str:
        """Add a comment to a PR.

        Args:
            pr_number: Pull request number
            body: Comment text (markdown supported)
            owner: Repository owner (default: GrubhubProd)
            repo: Repository name (default: events-mart)
        """
        try:
            session = _github_session()
            resp = session.post(
                f"{_GITHUB_API}/repos/{owner}/{repo}/issues/{pr_number}/comments",
                json={"body": body},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            return json.dumps({
                "result": "COMMENT_ADDED",
                "comment_id": data["id"],
                "url": data["html_url"],
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def github_update_pr(
        pr_number: int,
        body: str = "",
        title: str = "",
        owner: str = "GrubhubProd",
        repo: str = "events-mart",
    ) -> str:
        """Update a PR's title or body/description.

        Args:
            pr_number: Pull request number
            body: New PR body/description (empty = don't change)
            title: New PR title (empty = don't change)
            owner: Repository owner (default: GrubhubProd)
            repo: Repository name (default: events-mart)
        """
        try:
            session = _github_session()
            payload = {}
            if body:
                payload["body"] = body
            if title:
                payload["title"] = title
            if not payload:
                return json.dumps({"error": "Nothing to update. Provide body or title."})

            resp = session.patch(
                f"{_GITHUB_API}/repos/{owner}/{repo}/pulls/{pr_number}",
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            return json.dumps({
                "result": "UPDATED",
                "pr_number": data["number"],
                "url": data["html_url"],
            })
        except Exception as e:
            return json.dumps({"error": str(e)})
