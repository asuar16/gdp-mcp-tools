"""EMR cluster tools for GDP MCP server.

Lists and describes EMR clusters by scraping the CMDash web dashboard
(the same approach used by gdp-cli listemr).
"""

import json
import logging

import requests

import auth

logger = logging.getLogger(__name__)


def _fetch_cmdash_page(env, path=""):
    """Fetch a CMDash page and return the response text."""
    base = auth.cmdash_url(env)
    url = f"{base}/{path}" if path else base
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def _parse_cluster_table(html):
    """Parse the CMDash HTML page to extract cluster information.

    Uses pandas.read_html() to scrape the cluster table.
    """
    import pandas as pd

    tables = pd.read_html(html)
    if not tables:
        return []

    # CMDash typically has the cluster table as the first (or only) table
    df = tables[0]
    return df.to_dict(orient="records")


def register(mcp):

    @mcp.tool()
    def list_emr_clusters(env: str = "dev") -> str:
        """List active EMR clusters from the CMDash dashboard.

        Scrapes the cluster dashboard to show all running and recently
        terminated clusters.

        Args:
            env: Target environment - "dev" or "prod" (default: dev)
        """
        try:
            html = _fetch_cmdash_page(env)
        except Exception as e:
            return json.dumps({"error": f"Failed to fetch CMDash: {e}"})

        try:
            clusters = _parse_cluster_table(html)
        except ImportError:
            return json.dumps({"error": "pandas and lxml are required. Run: pip install pandas lxml"})
        except Exception as e:
            return json.dumps({"error": f"Failed to parse cluster data: {e}"})

        return json.dumps({
            "env": env,
            "clusters": clusters,
            "count": len(clusters),
        }, default=str)

    @mcp.tool()
    def describe_emr_cluster(name: str, env: str = "dev") -> str:
        """Get detailed information about a specific EMR cluster.

        Args:
            name: Cluster name (as shown in CMDash)
            env: Target environment - "dev" or "prod" (default: dev)
        """
        try:
            html = _fetch_cmdash_page(env, path=f"cluster/{name}")
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return json.dumps({"error": f"Cluster '{name}' not found"})
            return json.dumps({"error": f"Failed to fetch cluster details: {e}"})
        except Exception as e:
            return json.dumps({"error": f"Failed to fetch cluster details: {e}"})

        try:
            import pandas as pd

            tables = pd.read_html(html)
            if not tables:
                return json.dumps({"error": "No data tables found on cluster page"})

            # Combine all tables into a single result
            result = {"name": name, "env": env}
            for i, df in enumerate(tables):
                key = f"table_{i}" if i > 0 else "details"
                result[key] = df.to_dict(orient="records")

            return json.dumps(result, default=str)

        except ImportError:
            return json.dumps({"error": "pandas and lxml are required. Run: pip install pandas lxml"})
        except Exception as e:
            return json.dumps({"error": f"Failed to parse cluster details: {e}"})
