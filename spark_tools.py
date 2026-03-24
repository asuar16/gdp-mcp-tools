"""Spark History Server tools for GDP MCP server.

Fetches Spark application metrics from the Spark History Server REST API
for performance analysis during and after ETL runs.
"""

import json
import logging

import requests

import auth

logger = logging.getLogger(__name__)

# Spark History Server port (standard)
SPARK_HS_PORT = 18080


def _spark_hs_url(cluster_host, env="dev"):
    """Build the Spark History Server base URL."""
    # Strip any protocol prefix
    host = cluster_host.replace("http://", "").replace("https://", "").split(":")[0]
    return f"http://{host}:{SPARK_HS_PORT}"


def _spark_get(url, timeout=30):
    """GET request to Spark History Server (no auth needed, HTTP)."""
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _format_bytes(b):
    """Format bytes to human readable."""
    if b is None:
        return "N/A"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(b) < 1024.0:
            return f"{b:.1f} {unit}"
        b /= 1024.0
    return f"{b:.1f} PB"


def _format_ms(ms):
    """Format milliseconds to human readable."""
    if ms is None:
        return "N/A"
    if ms < 1000:
        return f"{ms}ms"
    if ms < 60000:
        return f"{ms / 1000:.1f}s"
    return f"{ms / 60000:.1f}min"


def register(mcp):

    @mcp.tool()
    def spark_app_details(app_id: str, cluster_host: str, env: str = "dev") -> str:
        """Get Spark application overview: stages, executors, shuffle, GC, and memory metrics.

        Fetches comprehensive performance data from the Spark History Server REST API.
        Use this after an ETL job completes (or during execution) to analyze Spark performance.

        To find the app_id and cluster_host: check the Azkaban job logs (get_flow_logs)
        and look for the YARN application URL like:
          http://<cluster>:18080/history/application_1234567890_0001

        Args:
            app_id: Spark application ID (e.g. "application_1234567890_0001")
            cluster_host: Cluster hostname (e.g. "ip-10-0-1-123.ec2.internal" or from Azkaban logs)
            env: Target environment - "dev" or "prod" (default: dev)
        """
        base = _spark_hs_url(cluster_host, env)

        try:
            # 1. Application info
            app_info = _spark_get(f"{base}/api/v1/applications/{app_id}")

            # 2. All stages
            stages = _spark_get(f"{base}/api/v1/applications/{app_id}/stages")

            # 3. Executors
            executors = _spark_get(f"{base}/api/v1/applications/{app_id}/allexecutors")

            # 4. Environment (Spark config)
            env_info = _spark_get(f"{base}/api/v1/applications/{app_id}/environment")

        except requests.exceptions.ConnectionError:
            return json.dumps({
                "error": f"Cannot connect to Spark History Server at {base}. "
                         "Check: 1) VPN is connected 2) Cluster is still running 3) Hostname is correct"
            })
        except requests.exceptions.HTTPError as e:
            return json.dumps({"error": f"Spark History Server returned {e.response.status_code}: {e}"})
        except Exception as e:
            return json.dumps({"error": f"Failed to fetch Spark metrics: {e}"})

        # -- Summarize stages --
        stage_summary = []
        total_input = 0
        total_output = 0
        total_shuffle_read = 0
        total_shuffle_write = 0
        total_gc = 0
        total_spill_memory = 0
        total_spill_disk = 0
        slowest_stage = None
        slowest_duration = 0

        for s in stages:
            duration = s.get("executorRunTime", 0)
            input_bytes = s.get("inputBytes", 0)
            output_bytes = s.get("outputBytes", 0)
            shuffle_read = s.get("shuffleReadBytes", 0)
            shuffle_write = s.get("shuffleWriteBytes", 0)
            gc_time = s.get("jvmGcTime", 0)
            spill_mem = s.get("memoryBytesSpilled", 0)
            spill_disk = s.get("diskBytesSpilled", 0)
            num_tasks = s.get("numCompleteTasks", 0) + s.get("numFailedTasks", 0)

            total_input += input_bytes
            total_output += output_bytes
            total_shuffle_read += shuffle_read
            total_shuffle_write += shuffle_write
            total_gc += gc_time
            total_spill_memory += spill_mem
            total_spill_disk += spill_disk

            if duration > slowest_duration:
                slowest_duration = duration
                slowest_stage = s

            stage_summary.append({
                "stage_id": s.get("stageId"),
                "name": s.get("name", "")[:80],
                "status": s.get("status"),
                "tasks": num_tasks,
                "failed_tasks": s.get("numFailedTasks", 0),
                "duration": _format_ms(duration),
                "input": _format_bytes(input_bytes),
                "output": _format_bytes(output_bytes),
                "shuffle_read": _format_bytes(shuffle_read),
                "shuffle_write": _format_bytes(shuffle_write),
                "gc_time": _format_ms(gc_time),
                "spill_memory": _format_bytes(spill_mem),
                "spill_disk": _format_bytes(spill_disk),
            })

        # -- Summarize executors --
        executor_summary = []
        for ex in executors:
            if ex.get("id") == "driver":
                continue
            peak_mem = ex.get("peakMemoryMetrics", {})
            executor_summary.append({
                "id": ex.get("id"),
                "host": ex.get("hostPort", ""),
                "cores": ex.get("totalCores"),
                "active_tasks": ex.get("activeTasks", 0),
                "completed_tasks": ex.get("completedTasks", 0),
                "failed_tasks": ex.get("failedTasks", 0),
                "total_gc_time": _format_ms(ex.get("totalGCTime", 0)),
                "total_input": _format_bytes(ex.get("totalInputBytes", 0)),
                "total_shuffle_read": _format_bytes(ex.get("totalShuffleRead", 0)),
                "total_shuffle_write": _format_bytes(ex.get("totalShuffleWrite", 0)),
                "max_memory": _format_bytes(ex.get("maxMemory", 0)),
                "peak_jvm_heap": _format_bytes(peak_mem.get("JVMHeapMemory", 0)),
                "peak_on_heap_exec": _format_bytes(peak_mem.get("OnHeapExecutionMemory", 0)),
            })

        # -- Extract key Spark configs --
        spark_props = {}
        for item in env_info.get("sparkProperties", []):
            if len(item) >= 2:
                key, val = item[0], item[1]
                if any(k in key for k in [
                    "executor.memory", "executor.cores", "executor.instances",
                    "driver.memory", "shuffle.partitions", "default.parallelism",
                    "broadcast.threshold", "memory.fraction", "memory.storageFraction",
                    "adaptive", "dynamicAllocation", "maxPartitionBytes",
                ]):
                    spark_props[key] = val

        # -- Build result --
        app_name = ""
        app_duration = ""
        if isinstance(app_info, list) and app_info:
            app_name = app_info[0].get("name", "")
            attempts = app_info[0].get("attempts", [])
            if attempts:
                app_duration = _format_ms(attempts[0].get("duration", 0))
        elif isinstance(app_info, dict):
            app_name = app_info.get("name", "")
            attempts = app_info.get("attempts", [])
            if attempts:
                app_duration = _format_ms(attempts[0].get("duration", 0))

        result = {
            "app_id": app_id,
            "app_name": app_name,
            "app_duration": app_duration,
            "total_stages": len(stages),
            "totals": {
                "input": _format_bytes(total_input),
                "output": _format_bytes(total_output),
                "shuffle_read": _format_bytes(total_shuffle_read),
                "shuffle_write": _format_bytes(total_shuffle_write),
                "gc_time": _format_ms(total_gc),
                "spill_memory": _format_bytes(total_spill_memory),
                "spill_disk": _format_bytes(total_spill_disk),
            },
            "slowest_stage": {
                "stage_id": slowest_stage.get("stageId") if slowest_stage else None,
                "name": slowest_stage.get("name", "")[:80] if slowest_stage else None,
                "duration": _format_ms(slowest_duration),
            } if slowest_stage else None,
            "stages": stage_summary,
            "executors": executor_summary,
            "spark_config": spark_props,
        }

        # Cap output
        output = json.dumps(result, indent=2)
        if len(output) > 50000:
            output = output[:50000] + "\n... (truncated)"
        return output

    @mcp.tool()
    def spark_stage_details(
        app_id: str,
        stage_id: int,
        cluster_host: str,
        env: str = "dev",
        attempt: int = 0,
    ) -> str:
        """Get detailed metrics for a specific Spark stage, including task-level quantiles.

        Use this to drill into a slow or problematic stage identified by spark_app_details.
        Returns task distribution (p1, median, p99), data skew indicators, and GC pressure.

        Args:
            app_id: Spark application ID
            stage_id: Stage ID to inspect (from spark_app_details output)
            cluster_host: Cluster hostname
            env: Target environment (default: dev)
            attempt: Stage attempt number (default: 0)
        """
        base = _spark_hs_url(cluster_host, env)

        try:
            # Stage details
            stage = _spark_get(f"{base}/api/v1/applications/{app_id}/stages/{stage_id}/{attempt}")

            # Task quantiles
            quantiles = _spark_get(
                f"{base}/api/v1/applications/{app_id}/stages/{stage_id}/{attempt}"
                f"/taskSummary?quantiles=0.01,0.25,0.5,0.75,0.99"
            )

            # Slowest tasks (top 10 by runtime)
            slow_tasks = _spark_get(
                f"{base}/api/v1/applications/{app_id}/stages/{stage_id}/{attempt}"
                f"/taskList?sortBy=-runtime&length=10"
            )

            # Failed tasks (if any)
            failed_tasks = _spark_get(
                f"{base}/api/v1/applications/{app_id}/stages/{stage_id}/{attempt}"
                f"/taskList?status=failed&length=10"
            )

        except requests.exceptions.ConnectionError:
            return json.dumps({"error": f"Cannot connect to Spark History Server at {base}"})
        except requests.exceptions.HTTPError as e:
            return json.dumps({"error": f"HTTP {e.response.status_code}: {e}"})
        except Exception as e:
            return json.dumps({"error": f"Failed: {e}"})

        # -- Quantile analysis --
        def format_quantile(metric_data, fmt_fn=_format_ms):
            if not metric_data or not isinstance(metric_data, list):
                return "N/A"
            labels = ["p1", "p25", "p50", "p75", "p99"]
            return {labels[i]: fmt_fn(v) for i, v in enumerate(metric_data) if i < len(labels)}

        quantile_summary = {
            "executor_run_time": format_quantile(quantiles.get("executorRunTime")),
            "gc_time": format_quantile(quantiles.get("jvmGcTime")),
            "peak_memory": format_quantile(quantiles.get("peakExecutionMemory"), _format_bytes),
            "spill_memory": format_quantile(quantiles.get("memoryBytesSpilled"), _format_bytes),
            "spill_disk": format_quantile(quantiles.get("diskBytesSpilled"), _format_bytes),
        }

        shuffle_read_q = quantiles.get("shuffleReadMetrics", {})
        if shuffle_read_q:
            quantile_summary["shuffle_fetch_wait"] = format_quantile(shuffle_read_q.get("fetchWaitTime"))
            quantile_summary["shuffle_remote_read"] = format_quantile(
                shuffle_read_q.get("remoteBytesRead"), _format_bytes
            )

        # -- Data skew detection --
        run_times = quantiles.get("executorRunTime", [])
        skew_detected = False
        skew_ratio = None
        if run_times and len(run_times) >= 5:
            p50 = run_times[2] if run_times[2] > 0 else 1
            p99 = run_times[4]
            skew_ratio = round(p99 / p50, 1)
            skew_detected = skew_ratio > 5  # p99 is 5x the median

        # -- Slowest tasks summary --
        slow_task_summary = []
        for t in slow_tasks[:10]:
            metrics = t.get("taskMetrics", {})
            slow_task_summary.append({
                "task_id": t.get("taskId"),
                "executor": t.get("executorId"),
                "host": t.get("host", "")[:30],
                "duration": _format_ms(t.get("duration")),
                "gc_time": _format_ms(metrics.get("jvmGcTime", 0)),
                "input": _format_bytes(metrics.get("inputMetrics", {}).get("bytesRead", 0)),
                "shuffle_read": _format_bytes(metrics.get("shuffleReadMetrics", {}).get("bytesRead", 0)),
                "spill_memory": _format_bytes(metrics.get("memoryBytesSpilled", 0)),
                "spill_disk": _format_bytes(metrics.get("diskBytesSpilled", 0)),
            })

        result = {
            "stage_id": stage_id,
            "stage_name": stage.get("name", "")[:100],
            "status": stage.get("status"),
            "num_tasks": stage.get("numCompleteTasks", 0) + stage.get("numFailedTasks", 0),
            "num_failed": stage.get("numFailedTasks", 0),
            "duration": _format_ms(stage.get("executorRunTime", 0)),
            "input": _format_bytes(stage.get("inputBytes", 0)),
            "output": _format_bytes(stage.get("outputBytes", 0)),
            "shuffle_read": _format_bytes(stage.get("shuffleReadBytes", 0)),
            "shuffle_write": _format_bytes(stage.get("shuffleWriteBytes", 0)),
            "gc_time": _format_ms(stage.get("jvmGcTime", 0)),
            "spill_memory": _format_bytes(stage.get("memoryBytesSpilled", 0)),
            "spill_disk": _format_bytes(stage.get("diskBytesSpilled", 0)),
            "task_quantiles": quantile_summary,
            "data_skew": {
                "detected": skew_detected,
                "p99_to_p50_ratio": skew_ratio,
                "verdict": "DATA SKEW DETECTED - p99 task is {}x slower than median".format(skew_ratio)
                if skew_detected else "No significant skew",
            },
            "slowest_tasks": slow_task_summary,
            "failed_tasks": len(failed_tasks),
        }

        output = json.dumps(result, indent=2)
        if len(output) > 50000:
            output = output[:50000] + "\n... (truncated)"
        return output
