# GDP MCP Tools Reference

37 tools across 11 service categories for Grubhub Data Platform operations.

---

## Jenkins (11 tools)

Tools for prod-to-dev table sync, branch deployment, schedule validation, PR integration, and cluster management.

| Tool | Description | Key Args |
|------|-------------|----------|
| `sync_table` | Sync a prod table to dev (waits for completion) | `table_name`, `num_partitions`, `date_partition_key`, `partition_start_date`, `partition_end_date`, `drop_and_create` |
| `sync_table_async` | Trigger sync without waiting | Same as above |
| `check_sync_status` | Check status of an async sync build | `build_url` |
| `jenkins_deploy_branch` | Deploy a branch to dev Azkaban | `branch`, `project_name`, `run_unit_tests` |
| `jenkins_deploy` | Deploy a version to prod | `version`, `env` |
| `jenkins_integrate` | Trigger PR integration build | `branch`, `pull_request_id` |
| `jenkins_validate_schedule` | Validate schedule.json | `branch`, `project_name` |
| `jenkins_start_cluster` | Start an EMR cluster | `project_name` |
| `jenkins_stop_cluster` | Stop an EMR cluster | `project_name` |
| `jenkins_abort_build` | Abort a running Jenkins build | `build_url` |
| `get_jenkins_build_logs` | Get console output from a build | `build_url`, `tail_lines` |

### Sync Tips
- `date_partition_key` must match the table's actual partition column
- Common partition keys: `session_start_date`, `event_date`, `created_date`, `metric_date`, `dt`
- After sync with `drop_and_create=true`, use `hive.schema.table` in Trino queries (not `gdp.` catalog)
- `sync_table` maintains a daily cache -- won't re-sync a table already synced today

---

## Azkaban (6 tools)

Workflow orchestration -- run flows, monitor executions, inspect DAGs, cancel jobs.

| Tool | Description | Key Args |
|------|-------------|----------|
| `azkaban_run_flow` | Execute an Azkaban flow | `project`, `flow`, `env`, `jobs` (whitelist), `properties`, `concurrentOption` |
| `azkaban_status` | Get execution status | `exec_id`, `env` |
| `azkaban_cancel` | Cancel a running execution | `exec_id`, `env` |
| `azkaban_monitor` | Poll execution until complete | `exec_id`, `env`, `poll_interval`, `timeout` |
| `azkaban_flows` | List flows and inspect DAG | `project`, `flow`, `env` |
| `azkaban_list_executions` | List recent executions | `project`, `flow`, `env`, `limit` |

### Key Concepts
- `jobs` param is a **WHITELIST** -- pass the jobs you WANT to run (others auto-disabled)
- Dev uses `concurrentOption=concurrent` by default
- Project naming: `events-mart_{project_folder}` (e.g., `events-mart_diner_session_topics_loader`)

---

## Trino (1 tool)

SQL query execution against prod or dev Trino clusters.

| Tool | Description | Key Args |
|------|-------------|----------|
| `trino_query` | Execute SQL and return JSON results | `query`, `dev` (bool), `dialect` (optional transpilation) |

### Tips
- `dev=true` (default) queries dev cluster; `dev=false` queries prod
- Results capped at 500 rows / 50K characters
- Use `dialect="hive"` or `dialect="spark"` to auto-transpile to Trino SQL
- JSON functions: use `json_extract_scalar(col, '$.path')` (NOT `get_json_object`)
- Date literals: `DATE '2026-01-01'` (NOT string comparison)

---

## EMR (2 tools)

EMR cluster information via CMDash.

| Tool | Description | Key Args |
|------|-------------|----------|
| `list_emr_clusters` | List active EMR clusters | `env`, `name_filter` |
| `describe_emr_cluster` | Get cluster details | `cluster_id`, `env` |

---

## PR (1 tool)

GitHub Pull Request discussion threads.

| Tool | Description | Key Args |
|------|-------------|----------|
| `get_pr_discussions` | Fetch PR review comments and threads | `pr_number`, `repo` |

---

## Flowlogs (1 tool)

Retrieve Azkaban execution logs from cluster machines via SSH.

| Tool | Description | Key Args |
|------|-------------|----------|
| `get_flow_logs` | Fetch job logs from Azkaban execution | `exec_id`, `job_name`, `env`, `tail_lines` |

---

## VPN (3 tools)

F5 VPN management for macOS. Required before using any other GDP tool.

| Tool | Description | Key Args |
|------|-------------|----------|
| `vpn_connect` | Connect to Grubhub VPN | -- |
| `vpn_disconnect` | Disconnect VPN | -- |
| `vpn_status` | Check VPN connection status | -- |

### Watchdog
- `vpn_connect` enables a watchdog that checks every 30 min (weekdays 9AM-9PM IST)
- Auto-reopens browser if VPN drops
- Use `caffeinate -s` to prevent macOS sleep

---

## Jira (5 tools)

Jira issue management for Atlassian Cloud.

| Tool | Description | Key Args |
|------|-------------|----------|
| `jira_search` | Search issues with JQL | `jql`, `max_results` |
| `jira_get_issue` | Get full issue details | `issue_key` |
| `jira_create_issue` | Create a new issue | `project`, `summary`, `description`, `issue_type` |
| `jira_transition_issue` | Move issue to next status | `issue_key`, `transition_name` |
| `jira_add_comment` | Add comment to issue | `issue_key`, `comment` |

### Transition Names (exact)
`New` -> `Refine` -> `Start Dev` -> `Submit for Review` -> `Passed Review` -> `Close Issue`

---

## Slack (1 tool)

Post messages to Slack channels.

| Tool | Description | Key Args |
|------|-------------|----------|
| `slack_post` | Send a message to a channel | `channel`, `message` |

---

## Spark (2 tools)

Spark History Server metrics for performance analysis.

| Tool | Description | Key Args |
|------|-------------|----------|
| `spark_app_details` | Application overview: stages, executors, shuffle, GC | `app_id`, `cluster_host`, `env` |
| `spark_stage_details` | Stage-level drill-down: task quantiles, data skew | `app_id`, `stage_id`, `cluster_host` |

### Usage
Find `app_id` and `cluster_host` from Azkaban job logs (look for YARN application URL).

---

## Sync Planning (2 tools)

Smart sync planning -- calculates optimal partitions to sync based on pipeline dependencies.

| Tool | Description | Key Args |
|------|-------------|----------|
| `calculate_sync_plan` | Generate a sync plan for a pipeline run | `project_name`, `run_date`, `num_days_load` |
| `check_sync_status` | Check which tables in a plan are synced | `plan_id` |

---

## PV Analysis (4 tools)

Production Validation (PV) framework failure analysis -- automates the 6-step methodology for investigating metric failures across 500+ daily checks.

| Tool | Description | Key Args |
|------|-------------|----------|
| `pv_failure_summary` | Overview of all PV failures for a date range | `days` (default 7) |
| `pv_analyze_metric` | Deep-dive: decompose CVR, find inflection, classify root cause | `metric_name`, `segment`, `days` |
| `pv_generate_report` | Generate styled HTML report with SVG charts | `metric_pattern`, `days` |
| `pv_investigate_root_cause` | Git + upstream analysis to determine code vs data change | `metric_name`, `segment`, `inflection_date` |

### `pv_failure_summary`
Queries `clickstream_pv_metric_analysis` for failures, groups by metric + segment, categorizes as CVR / Non-Null / Count WoW / Data Quality. Returns daily fail rates and recurring failures (3+ days).

### `pv_analyze_metric`
- Fetches PASS/FAIL trend for a specific metric
- Reads `table_validations.json` to find the SQL formula
- For CVR metrics: queries the source table to decompose into numerator/denominator
- Detects inflection point (first FAIL after PASS)
- Classifies root cause:
  - **Denominator Spike**: denominator increased, numerator flat (upstream volume change)
  - **Numerator Drop**: conversions dropped, traffic flat (real product issue)
  - **Traffic Drop**: both dropped proportionally (seasonal/outage)
  - **Mixed Signal**: both changed unpredictably

### `pv_generate_report`
Generates a dark-themed HTML report matching the quality of manually-crafted analysis reports:
- Executive summary with "NOT a pipeline bug" callout for upstream changes
- Timeline with actual metric values from decomposition data
- Dual SVG line charts: CVR % with threshold line (top) + Volume trends (bottom)
- All Recurring Failures table
- Data lineage diagram (auto-generated from known upstream chains)
- Recommendations table with pros/cons and auto-correction date
- Validation SQL queries

### `pv_investigate_root_cause`
Automated root cause investigation:

| Step | What it checks | How |
|------|---------------|-----|
| 1. Loader Code | `.py` files in `jobs/{pipeline}/` changed near inflection | `git log --after --before` |
| 2. Config | `schedule.json`, `table_validations.json`, `project.py` changes | `git log` |
| 3. Upstream Volumes | Row counts pre/post inflection for upstream tables (total + group-level) | Trino `COUNT(*)` + `GROUP BY impression_type` for known tables |
| 4. Column Blame | Commits that added/modified numerator/denominator columns | `git log -S {column}` |

Classification:
- `UPSTREAM_DATA_CHANGE` -- no code changes + volume anomaly
- `OUR_CODE_CHANGE` -- loader code changed + no volume anomaly
- `CONFIG_ONLY_CHANGE` -- only schedule/config changes (unlikely to cause failures)
- `MIXED_SIGNAL` -- both code + volume changes
- `INCONCLUSIVE` -- volume checks failed (connection errors)
- `NO_CHANGES_DETECTED` -- possible upstream service behavior change

---

## Common Workflows

### Deploy and Test a Branch
```
1. vpn_connect
2. jenkins_deploy_branch(branch="feature-branch", project_name="diner_session_topics")
3. azkaban_run_flow(project="events-mart_diner_session_topics_loader", flow="diner_session_topics_cleanup", ...)
4. azkaban_monitor(exec_id=...)
5. trino_query("SELECT count(*) FROM hive.integrated_events.diner_session_topics WHERE event_date = '2026-03-20'")
```

### Investigate PV Failures
```
1. pv_failure_summary(days=7)                           -- what's failing?
2. pv_analyze_metric("dslp_to_menu_cvr", "iOS Native")  -- why is it failing?
3. pv_investigate_root_cause("dslp_to_menu_cvr", "iOS Native", "2026-03-09")  -- our code or upstream?
4. pv_generate_report("discovery", days=14)              -- full HTML report
```

### Sync and Query
```
1. sync_table("integrated_events.diner_session_summary", date_partition_key="session_start_date", partition_start_date="2026-03-15", partition_end_date="2026-03-20")
2. trino_query("SELECT session_start_date, count(*) FROM hive.integrated_events.diner_session_summary WHERE session_start_date >= DATE '2026-03-15' GROUP BY 1 ORDER BY 1", dev=true)
```
