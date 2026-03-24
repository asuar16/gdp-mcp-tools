"""Trino query tool for GDP MCP server.

Executes SQL queries against prod or dev Trino clusters.
Supports optional dialect transpilation via sqlglot.
Results are capped at 500 rows / 50K characters to prevent response blowout.
"""

import json
import logging

import auth

logger = logging.getLogger(__name__)

MAX_ROWS = 500
MAX_CHARS = 50000


def register(mcp):

    @mcp.tool()
    def trino_query(
        query: str,
        dev: bool = True,
        dialect: str = "",
    ) -> str:
        """Execute a SQL query against Trino and return results as JSON.

        Results are capped at 500 rows and 50K characters. For large result
        sets, add LIMIT to your query.

        Args:
            query: SQL query to execute
            dev: Run against dev cluster (true) or prod cluster (false). Default: true
            dialect: Optional source dialect for sqlglot transpilation (e.g. "hive", "spark"). If set, the query is transpiled from this dialect to Trino SQL before execution.
        """
        # Optional: transpile from another dialect to Trino
        if dialect:
            try:
                import sqlglot
                query = sqlglot.transpile(query, read=dialect, write="trino")[0]
                logger.info("Transpiled query from %s to trino", dialect)
            except ImportError:
                return json.dumps({"error": "sqlglot is not installed. Install it to use dialect transpilation."})
            except Exception as e:
                return json.dumps({"error": f"sqlglot transpilation failed: {e}"})

        try:
            conn = auth.trino_connection(dev=dev)
        except RuntimeError as e:
            return json.dumps({"error": str(e)})
        except ImportError:
            return json.dumps({"error": "trino package is not installed. Run: pip install trino"})

        cursor = None
        try:
            cursor = conn.cursor()
            cursor.execute(query)

            # Fetch column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            # Fetch rows up to limit
            rows = []
            for row in cursor:
                rows.append(dict(zip(columns, row)))
                if len(rows) >= MAX_ROWS:
                    break

            truncated = cursor.fetchone() is not None if len(rows) == MAX_ROWS else False

            result = json.dumps({
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "truncated": truncated,
                "cluster": "dev" if dev else "prod",
            }, default=str)

            # Cap total response size
            if len(result) > MAX_CHARS:
                # Reduce rows until under limit
                while len(rows) > 1:
                    rows.pop()
                    result = json.dumps({
                        "columns": columns,
                        "rows": rows,
                        "row_count": len(rows),
                        "truncated": True,
                        "cluster": "dev" if dev else "prod",
                        "note": "Response truncated to fit size limit",
                    }, default=str)
                    if len(result) <= MAX_CHARS:
                        break

            return result

        except Exception as e:
            return json.dumps({"error": f"Query failed: {e}"})
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass
