"""PR discussion tools for GDP MCP server.

Fetches GitHub PR review threads and comments using the gh CLI

"""

import json
import logging
import subprocess

logger = logging.getLogger(__name__)


def _gh_api(endpoint):
    """Call the GitHub API via the gh CLI and return parsed JSON."""
    result = subprocess.run(
        ["gh", "api", endpoint, "--paginate"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gh api failed: {result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else []


def _format_comment(comment, context_lines=0):
    """Format a single PR comment into a readable dict."""
    formatted = {
        "author": comment.get("user", {}).get("login", "unknown"),
        "created_at": comment.get("created_at", ""),
        "body": comment.get("body", ""),
    }

    # For review comments (on specific lines of code)
    if comment.get("path"):
        formatted["file"] = comment.get("path")
        formatted["line"] = comment.get("line") or comment.get("original_line")
        formatted["side"] = comment.get("side", "")

        if context_lines > 0 and comment.get("diff_hunk"):
            # Show the last N lines of the diff hunk for context
            hunk_lines = comment["diff_hunk"].split("\n")
            formatted["diff_context"] = "\n".join(hunk_lines[-context_lines:])

    # Thread info
    if comment.get("in_reply_to_id"):
        formatted["reply_to"] = comment["in_reply_to_id"]

    return formatted


def register(mcp):

    @mcp.tool()
    def get_pr_discussions(
        pr_number: int,
        owner: str = "GrubhubProd",
        repo: str = "events-mart",
        filter_robots: bool = True,
        context_lines: int = 3,
    ) -> str:
        """Fetch GitHub PR discussion threads as markdown-formatted output.

        Returns all review comments and issue comments organized by thread.
        Requires the gh CLI to be installed and authenticated.

        Args:
            pr_number: Pull request number
            owner: Repository owner (default: GrubhubProd)
            repo: Repository name (default: events-mart)
            filter_robots: Filter out bot/CI comments (default: true)
            context_lines: Number of diff context lines to include (default: 3)
        """
        robot_users = {
            "github-actions[bot]", "codecov[bot]", "dependabot[bot]",
            "sonarcloud[bot]", "jenkins-bot", "gdp-bot",
        }

        try:
            # Fetch review comments (on specific code lines)
            review_comments = _gh_api(
                f"repos/{owner}/{repo}/pulls/{pr_number}/comments"
            )
            # Fetch issue-level comments (general discussion)
            issue_comments = _gh_api(
                f"repos/{owner}/{repo}/issues/{pr_number}/comments"
            )
            # Fetch reviews (approval/changes-requested/comment)
            reviews = _gh_api(
                f"repos/{owner}/{repo}/pulls/{pr_number}/reviews"
            )
        except RuntimeError as e:
            return json.dumps({"error": str(e)})
        except FileNotFoundError:
            return json.dumps({"error": "gh CLI not found. Install it: https://cli.github.com/"})
        except Exception as e:
            return json.dumps({"error": f"Failed to fetch PR data: {e}"})

        # Filter robot comments
        if filter_robots:
            review_comments = [
                c for c in review_comments
                if c.get("user", {}).get("login", "") not in robot_users
            ]
            issue_comments = [
                c for c in issue_comments
                if c.get("user", {}).get("login", "") not in robot_users
            ]
            reviews = [
                r for r in reviews
                if r.get("user", {}).get("login", "") not in robot_users
            ]

        # Group review comments into threads
        threads = {}
        for comment in review_comments:
            # Thread root is identified by in_reply_to_id or the comment's own id
            thread_id = comment.get("in_reply_to_id") or comment["id"]
            if thread_id not in threads:
                threads[thread_id] = []
            threads[thread_id].append(
                _format_comment(comment, context_lines)
            )

        # Format reviews (approval status)
        review_summary = []
        for review in reviews:
            if review.get("state") in ("APPROVED", "CHANGES_REQUESTED", "COMMENTED"):
                entry = {
                    "author": review.get("user", {}).get("login", "unknown"),
                    "state": review.get("state"),
                    "submitted_at": review.get("submitted_at", ""),
                }
                if review.get("body"):
                    entry["body"] = review["body"]
                review_summary.append(entry)

        # Format issue-level comments
        general_comments = [
            _format_comment(c, 0) for c in issue_comments
        ]

        result = {
            "pr_number": pr_number,
            "repo": f"{owner}/{repo}",
            "review_threads": list(threads.values()),
            "review_thread_count": len(threads),
            "reviews": review_summary,
            "general_comments": general_comments,
            "general_comment_count": len(general_comments),
        }

        output = json.dumps(result, default=str)

        # Cap response size
        if len(output) > 50000:
            result["note"] = "Response truncated. Use filter_robots=true or check PR directly."
            result["review_threads"] = result["review_threads"][:20]
            result["general_comments"] = result["general_comments"][:20]
            output = json.dumps(result, default=str)

        return output
