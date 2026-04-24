"""Jenkins tools for GDP MCP server.

Includes prod-dev table sync (ported from prod_dev_sync server) and
general Jenkins job tools: deploy-branch, deploy, validate-schedule,
integrate, start-cluster, stop-cluster.
"""

import json
import logging
import os
import re
import time
from datetime import date, datetime

import requests

import auth

logger = logging.getLogger(__name__)

JENKINS_TIMEOUT_SEC = int(os.environ.get("JENKINS_TIMEOUT_SEC", "900"))
JENKINS_POLL_INTERVAL_SEC = int(os.environ.get("JENKINS_POLL_INTERVAL_SEC", "3"))

# ---------------------------------------------------------------------------
# Sync tracker: daily cache of synced tables to avoid redundant syncs
# {table_name: {date, sync_type, sync_time, latest_partition, build_url}}
# ---------------------------------------------------------------------------
_sync_cache = {}


def _sync_type_label(params):
    """Derive sync type from params."""
    if params.get("use_date_range_partition_sync"):
        return f"date_range({params.get('date_partition_key')}: {params.get('partition_start_date')} to {params.get('partition_end_date')})"
    if params.get("use_specific_partition_column"):
        return f"partition({params.get('partition_column')}={params.get('partition_value')})"
    return "full"


def _record_sync(table_name, params, build_url, latest_partition=None):
    """Record a successful sync in the daily cache."""
    _sync_cache[table_name] = {
        "date": str(date.today()),
        "sync_type": _sync_type_label(params),
        "sync_time": datetime.now().strftime("%H:%M:%S"),
        "latest_partition": latest_partition,
        "build_url": build_url,
    }


def _check_sync_cache(table_name):
    """Check if a table was already synced today. Returns cache entry or None."""
    entry = _sync_cache.get(table_name)
    if entry and entry.get("date") == str(date.today()):
        return entry
    return None


def update_sync_partition(table_name, partition_column, partition_value):
    """Update the latest partition info for a synced table (called after Trino query)."""
    entry = _sync_cache.get(table_name)
    if entry:
        entry["latest_partition"] = {"column": partition_column, "value": str(partition_value)}


def get_sync_log():
    """Return today's sync log."""
    today = str(date.today())
    return {k: v for k, v in _sync_cache.items() if v.get("date") == today}

SYNC_JOB_PATH = os.environ.get(
    "SYNC_JOB_PATH",
    "job/data_platform/job/Tasks/job/prod_dev_sync/job/prod-dev-table-sync",
)


# ---------------------------------------------------------------------------
# Jenkins REST helpers (same approach as gdp-cli: raw requests + Basic Auth)
# ---------------------------------------------------------------------------
def _job_url(env, job_path):
    """Full URL to a Jenkins job."""
    return f"{auth.jenkins_url(env)}/{job_path}"


def _running_jobs(session, env, job_path):
    """Check if user already has a running build. Returns list of build URLs."""
    url = _job_url(env, job_path)
    resp = session.get(
        url + "/api/json?tree=builds[number,url,building,actions[causes[userId]]]"
    )
    resp.raise_for_status()
    user = auth.get_username()
    urls = []
    for build in resp.json().get("builds", []):
        if not build.get("building") or build.get("number") == 0:
            continue
        for action in build.get("actions", []):
            if any(c.get("userId") == user for c in action.get("causes", [])):
                urls.append(build["url"])
                break
    return urls


def _queued_jobs(session, env, job_path):
    """Check if job is already queued. Returns list of queue item IDs."""
    url = _job_url(env, job_path)
    resp = session.get(
        auth.jenkins_url(env) + "/queue/api/json?tree=items[id,task[name,url]]"
    )
    resp.raise_for_status()
    return [
        j["id"]
        for j in resp.json()["items"]
        if j["task"]["url"].rstrip("/") == url
    ]


def _submit_build(session, env, job_path, params, check_existing=True):
    """Submit a build, optionally reusing an existing running/queued build.

    Returns (build_url, queue_item_id).
    If attaching to a running build, queue_item_id is None.
    """
    url = _job_url(env, job_path)

    if check_existing:
        running = _running_jobs(session, env, job_path)
        if running:
            logger.info("Found already-running build: %s", running[0])
            return running[0], None

        queued = _queued_jobs(session, env, job_path)
        if queued:
            logger.info("Found already-queued build (queue_id=%s)", queued[0])
            return "", queued[0]

    resp = session.post(url + "/buildWithParameters", data=params)
    resp.raise_for_status()
    item_id = int(resp.headers["Location"].split("/")[-2])
    logger.info("Build queued (queue_id=%s)", item_id)
    return "", item_id


def _wait_for_queue(session, env, item_id):
    """Wait for a queued item to start executing. Returns the build URL."""
    deadline = time.time() + 120
    while time.time() < deadline:
        resp = session.get(
            f"{auth.jenkins_url(env)}/queue/item/{item_id}/api/json"
        )
        if resp.status_code == 200:
            data = resp.json()
            if "executable" in data and data["executable"]:
                build_url = data["executable"]["url"]
                logger.info("Build started: %s", build_url)
                return build_url
        time.sleep(1.5)
    raise TimeoutError(f"Build did not leave queue within 120s (queue_id={item_id})")


def _stream_logs_until_done(session, build_url, timeout=None, poll_interval=None):
    """Poll progressive log API until build completes.

    Returns (full_log_text, exit_status).
    """
    if timeout is None:
        timeout = JENKINS_TIMEOUT_SEC
    if poll_interval is None:
        poll_interval = JENKINS_POLL_INTERVAL_SEC

    logs_url = build_url.rstrip("/") + "/logText/progressiveText"
    full_log = []
    text_size = "0"
    deadline = time.time() + timeout

    resp = session.get(logs_url)
    if resp.text.strip():
        full_log.append(resp.text)
    text_size = resp.headers.get("X-Text-Size", "0")

    while "X-More-Data" in resp.headers:
        if time.time() > deadline:
            raise TimeoutError(f"Build did not complete within {timeout}s")
        time.sleep(poll_interval)
        resp = session.post(logs_url, data={"start": text_size})
        text_size = resp.headers.get("X-Text-Size", text_size)
        if resp.text.strip():
            full_log.append(resp.text)

    combined = "".join(full_log)
    exit_match = re.findall(r"^Finished: (.*)$", combined, re.MULTILINE)
    exit_status = exit_match[-1].strip() if exit_match else "UNKNOWN"
    return combined, exit_status


def _get_build_info(session, build_url):
    """Get build info JSON from a build URL."""
    resp = session.get(build_url.rstrip("/") + "/api/json")
    resp.raise_for_status()
    return resp.json()


def _get_console_text(session, build_url):
    """Get full console text from a build URL."""
    resp = session.get(build_url.rstrip("/") + "/consoleText")
    resp.raise_for_status()
    return resp.text


def _build_sync_params(
    table_name,
    num_partitions=5,
    partition_column=None,
    partition_value=None,
    drop_and_create_table=False,
    date_partition_key=None,
    partition_start_date=None,
    partition_end_date=None,
):
    """Build the Jenkins sync job parameters dict."""
    return {
        "table_name": table_name,
        "num_partitions": num_partitions,
        "use_specific_partition_column": partition_column is not None,
        "partition_column": partition_column,
        "partition_value": partition_value,
        "drop_and_create_table": drop_and_create_table,
        "use_date_range_partition_sync": date_partition_key is not None,
        "date_partition_key": date_partition_key,
        "partition_start_date": partition_start_date,
        "partition_end_date": partition_end_date,
    }


def _is_schema_mismatch(log_text):
    """Check if failure was caused by a schema mismatch."""
    markers = [
        "Exception: Aborting the job, schema change detected",
        "schema mismatch",
        "incompatible schema",
    ]
    lower = log_text.lower()
    return any(m.lower() in lower for m in markers)


def _extract_sync_log_summary(log_text):
    """Extract key info from sync build log: partitions synced, validation result."""
    summary = {}
    for line in log_text.split("\n"):
        if "list of partitions that requested for sync" in line.lower():
            # Extract partition list
            start = line.find("[")
            end = line.find("]")
            if start >= 0 and end >= 0:
                raw = line[start:end + 1]
                # Parse partition names from tuples like ('event_date=2026-03-05',)
                parts = re.findall(r"'([^']+)'", raw)
                summary["partitions_synced"] = parts
                summary["num_partitions"] = len(parts)
        elif "no. of partitions requested to sync" in line.lower():
            match = re.search(r"(\d+)", line)
            if match:
                summary["num_partitions"] = int(match.group(1))
        elif "validation complete" in line.lower():
            summary["validation"] = line.strip().split("INFO:prod_dev_sync:")[-1] if "INFO:" in line else line.strip()
        elif "successfully synced" in line.lower():
            summary["sync_status"] = line.strip().split("INFO:prod_dev_sync:")[-1] if "INFO:" in line else line.strip()
    return summary


def _run_jenkins_build(env, job_path, params, check_existing=True):
    """Common pattern: submit build, wait for queue, stream logs, return JSON result.

    Used by deploy/validate/integrate/cluster tools that all follow the same flow.
    """
    session = auth.jenkins_session(env)

    build_url, queue_id = _submit_build(
        session, env, job_path, params, check_existing
    )

    if queue_id is not None:
        build_url = _wait_for_queue(session, env, queue_id)

    log_text, exit_status = _stream_logs_until_done(session, build_url)

    if exit_status == "SUCCESS":
        return json.dumps({
            "build_url": build_url,
            "result": "SUCCESS",
        })

    tail = log_text[-3000:] if len(log_text) > 3000 else log_text
    return json.dumps({
        "build_url": build_url,
        "result": exit_status,
        "console_tail": tail,
    })


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------
def register(mcp):

    # -----------------------------------------------------------------------
    # Sync tools (ported from prod_dev_sync/server.py)
    # -----------------------------------------------------------------------
    @mcp.tool()
    def sync_table(
        table_name: str,
        num_partitions: int = 5,
        partition_column: str = "",
        partition_value: str = "",
        drop_and_create: bool = False,
        date_partition_key: str = "",
        partition_start_date: str = "",
        partition_end_date: str = "",
    ) -> str:
        """Trigger a prod-to-dev table sync and wait for it to complete.

        Args:
            table_name: Fully qualified table name (schema.table), e.g. "integrated_events.clickstream_v2_events"
            num_partitions: Number of partitions to sync (default 5, leave as default for non-partitioned tables)
            partition_column: Partition column to sync a specific partition (must also set partition_value)
            partition_value: Partition value to sync (must also set partition_column)
            drop_and_create: Drop and recreate the dev table before syncing (resolves schema mismatches)
            date_partition_key: Date partition key for date range sync (must also set partition_start_date and partition_end_date)
            partition_start_date: Start date for date range sync (YYYY-MM-DD)
            partition_end_date: End date for date range sync (YYYY-MM-DD)
        """
        if "." not in table_name:
            return json.dumps({"error": "table_name must be 'schema.table' format (e.g. 'integrated_events.clickstream_v2_events')"})

        p_col = partition_column or None
        p_val = partition_value or None
        if (p_col or p_val) and not (p_col and p_val):
            return json.dumps({"error": "Both partition_column and partition_value must be specified together"})

        d_key = date_partition_key or None
        d_start = partition_start_date or None
        d_end = partition_end_date or None
        if (d_key or d_start or d_end) and not (d_key and d_start and d_end):
            return json.dumps({"error": "All of date_partition_key, partition_start_date, and partition_end_date must be specified together"})

        # Check if already synced today (skip redundant syncs)
        cached = _check_sync_cache(table_name)
        if cached and not drop_and_create:
            logger.info("Table %s already synced today at %s, skipping", table_name, cached["sync_time"])
            return json.dumps({
                "result": "SKIPPED",
                "message": f"Table {table_name} was already synced today at {cached['sync_time']} ({cached['sync_type']}). Use drop_and_create=true to force re-sync.",
                "cached": cached,
            })

        params = _build_sync_params(
            table_name=table_name,
            num_partitions=num_partitions,
            partition_column=p_col,
            partition_value=p_val,
            drop_and_create_table=drop_and_create,
            date_partition_key=d_key,
            partition_start_date=d_start,
            partition_end_date=d_end,
        )

        try:
            session = auth.jenkins_session("dev")
        except RuntimeError as e:
            return json.dumps({"error": str(e)})

        try:
            build_url, queue_id = _submit_build(session, "dev", SYNC_JOB_PATH, params)
        except requests.HTTPError as e:
            return json.dumps({"error": f"Jenkins rejected the request (HTTP {e.response.status_code}). Check parameters."})
        except Exception as e:
            return json.dumps({"error": f"Failed to submit build: {e}"})

        if queue_id is not None:
            try:
                build_url = _wait_for_queue(session, "dev", queue_id)
            except TimeoutError as e:
                return json.dumps({"error": str(e)})

        try:
            log_text, exit_status = _stream_logs_until_done(session, build_url)
        except TimeoutError as e:
            return json.dumps({"build_url": build_url, "result": "TIMEOUT", "message": str(e)})

        # On failure, check for schema mismatch and auto-retry
        if exit_status != "SUCCESS" and not drop_and_create:
            if _is_schema_mismatch(log_text):
                logger.info("Schema mismatch detected. Retrying with drop_and_create=true...")
                params["drop_and_create_table"] = True
                try:
                    retry_build_url, retry_queue_id = _submit_build(session, "dev", SYNC_JOB_PATH, params)
                    if retry_queue_id is not None:
                        retry_build_url = _wait_for_queue(session, "dev", retry_queue_id)
                    retry_log, retry_status = _stream_logs_until_done(session, retry_build_url)
                    if retry_status == "SUCCESS":
                        _record_sync(table_name, params, retry_build_url)
                        return json.dumps({
                            "build_url": retry_build_url,
                            "result": "SUCCESS",
                            "schema_mismatch_retry": True,
                            "message": "Initial build failed due to schema mismatch. Retry with drop_and_create succeeded.",
                        })
                    tail = retry_log[-3000:] if len(retry_log) > 3000 else retry_log
                    return json.dumps({
                        "build_url": retry_build_url,
                        "result": retry_status,
                        "schema_mismatch_retry": True,
                        "console_tail": tail,
                    })
                except Exception as e:
                    logger.warning("Schema-mismatch retry failed: %s", e)

        if exit_status != "SUCCESS":
            tail = log_text[-3000:] if len(log_text) > 3000 else log_text
            return json.dumps({"build_url": build_url, "result": exit_status, "console_tail": tail})

        _record_sync(table_name, params, build_url)

        # Extract key details from build log for the response
        log_summary = _extract_sync_log_summary(log_text)
        return json.dumps({
            "build_url": build_url,
            "result": "SUCCESS",
            "message": f"Table {table_name} synced successfully from prod to dev.",
            **log_summary,
        })

    @mcp.tool()
    def sync_table_async(
        table_name: str,
        num_partitions: int = 5,
        partition_column: str = "",
        partition_value: str = "",
        drop_and_create: bool = False,
        date_partition_key: str = "",
        partition_start_date: str = "",
        partition_end_date: str = "",
    ) -> str:
        """Trigger a prod-to-dev table sync without waiting for completion (fire-and-forget).

        Returns the build URL immediately so you can poll with check_sync_status.

        Args:
            table_name: Fully qualified table name (schema.table), e.g. "integrated_events.clickstream_v2_events"
            num_partitions: Number of partitions to sync (default 5, leave as default for non-partitioned tables)
            partition_column: Partition column to sync a specific partition (must also set partition_value)
            partition_value: Partition value to sync (must also set partition_column)
            drop_and_create: Drop and recreate the dev table before syncing (resolves schema mismatches)
            date_partition_key: Date partition key for date range sync (must also set partition_start_date and partition_end_date)
            partition_start_date: Start date for date range sync (YYYY-MM-DD)
            partition_end_date: End date for date range sync (YYYY-MM-DD)
        """
        if "." not in table_name:
            return json.dumps({"error": "table_name must be 'schema.table' format"})

        p_col = partition_column or None
        p_val = partition_value or None
        if (p_col or p_val) and not (p_col and p_val):
            return json.dumps({"error": "Both partition_column and partition_value must be specified together"})

        d_key = date_partition_key or None
        d_start = partition_start_date or None
        d_end = partition_end_date or None
        if (d_key or d_start or d_end) and not (d_key and d_start and d_end):
            return json.dumps({"error": "All of date_partition_key, partition_start_date, and partition_end_date must be specified together"})

        params = _build_sync_params(
            table_name=table_name,
            num_partitions=num_partitions,
            partition_column=p_col,
            partition_value=p_val,
            drop_and_create_table=drop_and_create,
            date_partition_key=d_key,
            partition_start_date=d_start,
            partition_end_date=d_end,
        )

        try:
            session = auth.jenkins_session("dev")
        except RuntimeError as e:
            return json.dumps({"error": str(e)})

        try:
            build_url, queue_id = _submit_build(session, "dev", SYNC_JOB_PATH, params, check_existing=False)
        except requests.HTTPError as e:
            return json.dumps({"error": f"Jenkins rejected the request (HTTP {e.response.status_code})"})
        except Exception as e:
            return json.dumps({"error": f"Failed to submit build: {e}"})

        return json.dumps({
            "result": "QUEUED",
            "queue_id": queue_id,
            "message": f"Sync queued for {table_name}. Jenkins will process it in order.",
        })

    @mcp.tool()
    def check_sync_status(build_url: str) -> str:
        """Check the status of a previously triggered sync build (non-blocking).

        Args:
            build_url: The Jenkins build URL returned by sync_table or sync_table_async
        """
        try:
            session = auth.jenkins_session("dev")
        except RuntimeError as e:
            return json.dumps({"error": str(e)})

        try:
            info = _get_build_info(session, build_url)
        except Exception as e:
            return json.dumps({"error": f"Failed to get build info: {e}"})

        building = info.get("building", False)
        return json.dumps({
            "build_url": build_url,
            "building": building,
            "result": info.get("result"),
            "status": "RUNNING" if building else info.get("result", "UNKNOWN"),
            "duration_ms": info.get("duration"),
        })

    @mcp.tool()
    def get_jenkins_build_logs(build_url: str) -> str:
        """Fetch console output for any Jenkins build.

        Works with sync builds, deploy builds, integrate builds, or any Jenkins job.

        Args:
            build_url: The Jenkins build URL (e.g. from sync_table, jenkins_deploy_branch, etc.)
        """
        try:
            session = auth.jenkins_session("dev")
        except RuntimeError as e:
            return json.dumps({"error": str(e)})

        try:
            console_text = _get_console_text(session, build_url)
        except Exception as e:
            return json.dumps({"error": f"Failed to get console output: {e}"})

        if len(console_text) > 5000:
            console_text = "... (truncated, showing last 5000 chars) ...\n" + console_text[-5000:]

        return console_text

    # -----------------------------------------------------------------------
    # Deploy / Integrate / Cluster tools
    # -----------------------------------------------------------------------
    @mcp.tool()
    def jenkins_deploy_branch(
        branch: str,
        team: str = "data_platform",
        repo: str = "events-mart",
        project: str = "",
        no_unit_tests: bool = False,
        env: str = "dev",
    ) -> str:
        """Deploy a Git branch to Jenkins for testing.

        Triggers the deploy-branch Jenkins job which builds and deploys the
        specified branch to the target environment.

        Args:
            branch: Git branch name to deploy
            team: Jenkins team folder (default: data_platform)
            repo: Repository name (default: events-mart)
            project: Optional project name within the repo
            no_unit_tests: Skip unit tests during deploy (default: false)
            env: Target environment - "dev" or "prod" (default: dev)
        """
        job_path = f"job/{team}/job/Deployments/job/{repo}/job/deploy-branch-{repo}"
        params = {"branch": branch}
        if project:
            params["project_name"] = project
        params["run_unit_tests"] = str(not no_unit_tests).lower()
        params["run_schedule_json_validation"] = "false"

        try:
            return _run_jenkins_build(env, job_path, params)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def jenkins_deploy(
        version: str,
        team: str = "data_platform",
        repo: str = "events-mart",
        env: str = "dev",
    ) -> str:
        """Deploy a released version to the target environment.

        Args:
            version: Version string to deploy (e.g. "1.30.0")
            team: Jenkins team folder (default: data_platform)
            repo: Repository name (default: events-mart)
            env: Target environment - "dev" or "prod" (default: dev)
        """
        deploy_path_template = os.environ.get("DEPLOY_JOB_PATH_TEMPLATE", "job/experiments/job/Deployments/job/{repo}/job/deploy-{repo}")
        job_path = deploy_path_template.format(repo=repo)
        params = {"VERSION": version}

        try:
            return _run_jenkins_build(env, job_path, params)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def jenkins_validate_schedule(
        branch: str,
        team: str = "data_platform",
        repo: str = "events-mart",
        env: str = "dev",
    ) -> str:
        """Validate Azkaban schedule.json files for a branch.

        Runs the validate-schedule Jenkins job to check schedule configuration
        before merging.

        Args:
            branch: Git branch name containing schedule changes
            team: Jenkins team folder (default: data_platform)
            repo: Repository name (default: events-mart)
            env: Target environment - "dev" or "prod" (default: dev)
        """
        job_path = f"job/{team}/job/Tasks/job/{repo}/job/validate-schedule"
        params = {"BRANCH": branch}

        try:
            return _run_jenkins_build(env, job_path, params)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def jenkins_integrate(
        branch: str,
        team: str = "experiments",
        repo: str = "events-mart",
        pull_request_id: str = "",
        env: str = "dev",
    ) -> str:
        """Integrate (merge and deploy) a pull request branch.

        Triggers the integrate Jenkins job which merges the branch and
        deploys the result.

        Args:
            branch: Git branch name to integrate
            team: Jenkins team folder (default: experiments)
            repo: Repository name (default: events-mart)
            pull_request_id: GitHub PR number (optional, for tracking)
            env: Target environment - "dev" or "prod" (default: dev)
        """
        job_path = f"job/{team}/job/integrations/job/{repo}-integrate"
        params = {"branch": branch}
        if pull_request_id:
            params["pull_request_id"] = pull_request_id

        try:
            return _run_jenkins_build(env, job_path, params, check_existing=False)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def jenkins_start_cluster(
        team_name: str = "data_platform",
        cluster_type: str = "standard",
        node_count: int = 5,
        instance_type: str = "m5.4xlarge",
        max_uptime: int = 8,
        spot_instances: bool = True,
        env: str = "dev",
    ) -> str:
        """Start an EMR cluster via Jenkins.

        Args:
            team_name: Team that owns the cluster (default: data_platform)
            cluster_type: Cluster type - "standard" or "high-mem" (default: standard)
            node_count: Number of worker nodes (default: 5)
            instance_type: EC2 instance type (default: m5.4xlarge)
            max_uptime: Maximum cluster uptime in hours (default: 8)
            spot_instances: Use spot instances for workers (default: true)
            env: Target environment - "dev" or "prod" (default: dev)
        """
        job_path = f"job/{team_name}/job/Tasks/job/cluster_operations/job/start-cluster"
        params = {
            "CLUSTER_TYPE": cluster_type,
            "NODE_COUNT": str(node_count),
            "INSTANCE_TYPE": instance_type,
            "MAX_UPTIME": str(max_uptime),
            "SPOT_INSTANCES": str(spot_instances).lower(),
        }

        try:
            return _run_jenkins_build(env, job_path, params)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def jenkins_stop_cluster(env: str = "dev") -> str:
        """Stop the current user's running EMR cluster.

        Triggers the stop-cluster Jenkins job for the authenticated user.

        Args:
            env: Target environment - "dev" or "prod" (default: dev)
        """
        job_path = os.environ.get("STOP_CLUSTER_JOB_PATH", "job/data_platform/job/Tasks/job/cluster_operations/job/stop-cluster")

        try:
            return _run_jenkins_build(env, job_path, params={})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def jenkins_abort_build(build_url: str, env: str = "dev") -> str:
        """Abort a running Jenkins build.

        Sends a stop request to a Jenkins build that is currently in progress.
        Useful when new code was pushed after the build started (stale build).

        Args:
            build_url: The Jenkins build URL to abort (e.g. from jenkins_deploy_branch)
            env: Target environment - "dev" or "prod" (default: dev)
        """
        try:
            session = auth.jenkins_session(env)
        except RuntimeError as e:
            return json.dumps({"error": str(e)})

        try:
            # Check if build is actually running
            info = _get_build_info(session, build_url)
            if not info.get("building", False):
                return json.dumps({
                    "result": "NOT_RUNNING",
                    "message": f"Build is not running (status: {info.get('result', 'UNKNOWN')}). Nothing to abort.",
                    "build_url": build_url,
                })

            # Abort the build
            stop_url = build_url.rstrip("/") + "/stop"
            resp = session.post(stop_url)
            resp.raise_for_status()

            # Verify it stopped
            time.sleep(2)
            info = _get_build_info(session, build_url)
            return json.dumps({
                "result": "ABORTED" if not info.get("building") else "ABORT_SENT",
                "message": f"Build abort {'confirmed' if not info.get('building') else 'sent (may take a moment)'}.",
                "build_url": build_url,
                "build_status": info.get("result", "ABORTING"),
            })
        except Exception as e:
            return json.dumps({"error": f"Failed to abort build: {e}"})
