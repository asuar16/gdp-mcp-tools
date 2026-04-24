"""DataHub tools for querying dataset metadata, lineage, and ownership.

Uses the DataHub GraphQL API at /api/graphql with Bearer token auth.
Token is a Personal Access Token (PAT) generated from the DataHub UI
(Settings > Access Tokens) after logging in via Okta.

Env vars:
    DATAHUB_URL: DataHub base URL (default: https://datahub.gdp.data.grubhub.com)
    DATAHUB_TOKEN: Personal Access Token
"""

import json
import logging
import os

import requests

logger = logging.getLogger(__name__)


def _get_session():
    url = os.environ.get("DATAHUB_URL", "https://datahub.gdp.data.grubhub.com").rstrip("/")
    token = os.environ.get("DATAHUB_TOKEN", "")
    if not token:
        raise RuntimeError("DATAHUB_TOKEN must be set. Generate a PAT from DataHub UI: Settings > Access Tokens.")
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    return session, url


def _graphql(query, variables=None):
    session, url = _get_session()
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = session.post(f"{url}/api/graphql", json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        return {"error": data["errors"]}
    return data.get("data", {})


def _build_urn(table_name, platform="hive", env="PROD"):
    """Build a DataHub dataset URN from a table name like schema.table."""
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{table_name},{env})"


def register(mcp):

    @mcp.tool()
    def datahub_whoami() -> str:
        """Check DataHub connectivity and return the authenticated user."""
        try:
            result = _graphql("query { me { corpUser { urn username properties { displayName email } } } }")
            return json.dumps({"result": result})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def datahub_search(query: str, count: int = 20) -> str:
        """Search for datasets in DataHub.

        Args:
            query: Search term (e.g. 'diner_session_summary', 'clickstream')
            count: Max results to return (default 20)
        """
        try:
            gql = """
            query SearchDatasets($query: String!, $count: Int!) {
              searchAcrossEntities(input: {
                types: [DATASET]
                query: $query
                start: 0
                count: $count
                skipHighlighting: true
                skipAggregates: true
              }) {
                total
                searchResults {
                  entity {
                    urn
                    type
                    ... on Dataset {
                      name
                      platform { name }
                      properties { name description }
                      tags { tags { tag { name } } }
                      domain { domain { name } }
                    }
                  }
                }
              }
            }
            """
            result = _graphql(gql, {"query": query, "count": count})
            if "error" in result:
                return json.dumps(result)
            search = result.get("searchAcrossEntities", {})
            datasets = []
            for sr in search.get("searchResults", []):
                e = sr.get("entity", {})
                datasets.append({
                    "urn": e.get("urn"),
                    "name": e.get("name"),
                    "platform": (e.get("platform") or {}).get("name"),
                    "description": (e.get("properties") or {}).get("description", ""),
                    "tags": [t["tag"]["name"] for t in (e.get("tags") or {}).get("tags", [])],
                    "domain": (e.get("domain") or {}).get("domain", {}).get("name"),
                })
            return json.dumps({"total": search.get("total", 0), "datasets": datasets})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def datahub_get_dataset(table_name: str, platform: str = "hive", env: str = "PROD") -> str:
        """Get dataset metadata: schema, description, tags, owners, domain.

        Args:
            table_name: Fully qualified table name (e.g. 'integrated_events.diner_session_summary')
            platform: Data platform (default 'hive'). Other options: kafka, s3, etc.
            env: Environment (default 'PROD')
        """
        try:
            urn = _build_urn(table_name, platform, env)
            gql = """
            query GetDataset($urn: String!) {
              dataset(urn: $urn) {
                urn
                name
                platform { name }
                properties {
                  name
                  description
                  customProperties { key value }
                }
                editableProperties { description }
                schemaMetadata {
                  fields {
                    fieldPath
                    type
                    nativeDataType
                    description
                  }
                }
                tags { tags { tag { name } } }
                glossaryTerms { terms { term { name } } }
                ownership {
                  owners {
                    owner {
                      ... on CorpUser { username properties { displayName email } }
                      ... on CorpGroup { name }
                    }
                    type
                  }
                }
                domain { domain { name } }
                deprecation { deprecated decommissionTime }
              }
            }
            """
            result = _graphql(gql, {"urn": urn})
            if "error" in result:
                return json.dumps(result)
            ds = result.get("dataset")
            if not ds:
                return json.dumps({"error": f"Dataset not found: {urn}"})
            out = {
                "urn": ds["urn"],
                "name": ds.get("name"),
                "platform": (ds.get("platform") or {}).get("name"),
                "description": (ds.get("properties") or {}).get("description", ""),
                "editable_description": (ds.get("editableProperties") or {}).get("description", ""),
                "custom_properties": {
                    p["key"]: p["value"]
                    for p in (ds.get("properties") or {}).get("customProperties", [])
                },
                "schema": [
                    {
                        "field": f["fieldPath"],
                        "type": f.get("type"),
                        "native_type": f.get("nativeDataType"),
                        "description": f.get("description", ""),
                    }
                    for f in (ds.get("schemaMetadata") or {}).get("fields", [])
                ],
                "tags": [t["tag"]["name"] for t in (ds.get("tags") or {}).get("tags", [])],
                "glossary_terms": [t["term"]["name"] for t in (ds.get("glossaryTerms") or {}).get("terms", [])],
                "owners": [
                    {
                        "name": o["owner"].get("username") or o["owner"].get("name"),
                        "type": o.get("type"),
                        "display_name": (o["owner"].get("properties") or {}).get("displayName"),
                        "email": (o["owner"].get("properties") or {}).get("email"),
                    }
                    for o in (ds.get("ownership") or {}).get("owners", [])
                ],
                "domain": (ds.get("domain") or {}).get("domain", {}).get("name"),
                "deprecated": (ds.get("deprecation") or {}).get("deprecated", False),
            }
            return json.dumps({"result": out})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def datahub_get_lineage(table_name: str, direction: str = "UPSTREAM", platform: str = "hive", env: str = "PROD") -> str:
        """Get upstream or downstream lineage for a dataset.

        Args:
            table_name: Fully qualified table name (e.g. 'integrated_events.diner_session_summary')
            direction: 'UPSTREAM' (what feeds this table) or 'DOWNSTREAM' (what reads from it)
            platform: Data platform (default 'hive')
            env: Environment (default 'PROD')
        """
        try:
            urn = _build_urn(table_name, platform, env)
            gql = """
            query GetLineage($urn: String!, $direction: LineageDirection!) {
              scrollAcrossLineage(input: {
                query: "*"
                urn: $urn
                count: 100
                direction: $direction
                orFilters: [{
                  and: [{
                    condition: EQUAL
                    negated: false
                    field: "degree"
                    values: ["1", "2", "3+"]
                  }]
                }]
              }) {
                searchResults {
                  degree
                  entity {
                    urn
                    type
                    ... on Dataset {
                      name
                      platform { name }
                      properties { description }
                    }
                  }
                }
              }
            }
            """
            result = _graphql(gql, {"urn": urn, "direction": direction.upper()})
            if "error" in result:
                return json.dumps(result)
            scroll = result.get("scrollAcrossLineage", {})
            lineage = []
            for sr in scroll.get("searchResults", []):
                e = sr.get("entity", {})
                lineage.append({
                    "degree": sr.get("degree"),
                    "urn": e.get("urn"),
                    "name": e.get("name"),
                    "type": e.get("type"),
                    "platform": (e.get("platform") or {}).get("name"),
                    "description": (e.get("properties") or {}).get("description", ""),
                })
            lineage.sort(key=lambda x: (x.get("degree") or 0, x.get("name") or ""))
            return json.dumps({
                "table": table_name,
                "direction": direction.upper(),
                "count": len(lineage),
                "lineage": lineage,
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def datahub_get_column_lineage(table_name: str, platform: str = "hive", env: str = "PROD") -> str:
        """Get fine-grained column-level lineage for a dataset.

        Args:
            table_name: Fully qualified table name (e.g. 'integrated_events.diner_session_summary')
            platform: Data platform (default 'hive')
            env: Environment (default 'PROD')
        """
        try:
            urn = _build_urn(table_name, platform, env)
            gql = """
            query GetColumnLineage($urn: String!) {
              dataset(urn: $urn) {
                fineGrainedLineages {
                  upstreams {
                    urn
                    path
                  }
                  downstreams {
                    urn
                    path
                  }
                  transformOperation
                }
              }
            }
            """
            result = _graphql(gql, {"urn": urn})
            if "error" in result:
                return json.dumps(result)
            ds = result.get("dataset")
            if not ds:
                return json.dumps({"error": f"Dataset not found: {urn}"})
            lineages = ds.get("fineGrainedLineages") or []
            return json.dumps({
                "table": table_name,
                "column_lineage_count": len(lineages),
                "column_lineage": lineages,
            })
        except Exception as e:
            return json.dumps({"error": str(e)})
