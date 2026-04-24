"""S3 tools for GDP MCP server.

List partitions, check data existence, and inspect S3 paths for GDP tables.
"""

import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)

_BUCKETS = {
    "dev": "grubhub-dl-data-assets-dev",
    "prod": "grubhub-dl-data-assets-prod",
    "ods_dev": "grubhub-gdp-ods-data-assets-dev",
    "ods_prod": "grubhub-gdp-ods-data-assets-prod",
    "raw_prod": "dl-rawdata-prod",
    "raw_preprod": "dl-rawdata-preprod",
}


def register(mcp):

    @mcp.tool()
    def s3_list_partitions(
        database: str,
        table: str,
        env: str = "prod",
        prefix_override: str = "",
        max_partitions: int = 30,
    ) -> str:
        """List S3 partition folders for a GDP table.

        Args:
            database: Database name (e.g. "integrated_events", "ods")
            table: Table name (e.g. "diner_session_topics")
            env: Environment - "dev" or "prod" (default: prod)
            prefix_override: Override the S3 prefix path (for non-standard locations)
            max_partitions: Max partitions to return (default: 30, most recent)
        """
        try:
            if prefix_override:
                prefix = prefix_override
                bucket = prefix.split("/")[2] if prefix.startswith("s3://") else _BUCKETS.get(env, "")
                if prefix.startswith("s3://"):
                    prefix = "/".join(prefix.split("/")[3:])
            else:
                bucket = _BUCKETS.get(f"ods_{env}" if database == "ods" else env, "")
                prefix = f"{database}.db/{table}/"

            if not bucket:
                return json.dumps({"error": f"Unknown env: {env}"})

            s3 = boto3.client("s3")
            paginator = s3.get_paginator("list_objects_v2")
            result = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")

            partitions = []
            for page in result:
                for cp in page.get("CommonPrefixes", []):
                    partitions.append(cp["Prefix"].rstrip("/").split("/")[-1])

            partitions.sort(reverse=True)
            partitions = partitions[:max_partitions]

            return json.dumps({
                "result": "OK",
                "bucket": bucket,
                "prefix": prefix,
                "partitions": partitions,
                "count": len(partitions),
            })
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def s3_list_batch_ids(
        database: str,
        table: str,
        partition: str,
        env: str = "prod",
    ) -> str:
        """List batch_id subfolders within a partition.

        Args:
            database: Database name (e.g. "integrated_events")
            table: Table name (e.g. "diner_session_topics")
            partition: Partition folder (e.g. "event_date=2026-04-12")
            env: Environment - "dev" or "prod" (default: prod)
        """
        try:
            bucket = _BUCKETS.get(f"ods_{env}" if database == "ods" else env, "")
            prefix = f"{database}.db/{table}/{partition}/"

            s3 = boto3.client("s3")
            paginator = s3.get_paginator("list_objects_v2")
            result = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")

            batch_ids = []
            for page in result:
                for cp in page.get("CommonPrefixes", []):
                    batch_ids.append(cp["Prefix"].rstrip("/").split("/")[-1])

            return json.dumps({
                "result": "OK",
                "bucket": bucket,
                "prefix": prefix,
                "batch_ids": batch_ids,
                "count": len(batch_ids),
            })
        except Exception as e:
            return json.dumps({"error": str(e)})