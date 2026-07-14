-- Actual Genie-issued SQL work from system.query.history
-- Prefer genie_space_id (excludes warehouse idle / auto-stop billing noise).
-- Docs: https://docs.databricks.com/aws/en/admin/system-tables/query-history
SELECT
  qh.query_source.genie_space_id AS genie_space_id,
  qh.compute.warehouse_id AS warehouse_id,
  COUNT(*) AS query_count,
  SUM(COALESCE(qh.total_duration_ms, 0)) AS total_duration_ms,
  SUM(COALESCE(qh.execution_duration_ms, 0)) AS execution_duration_ms,
  SUM(COALESCE(qh.waiting_for_compute_duration_ms, 0)) AS waiting_for_compute_ms,
  SUM(COALESCE(qh.waiting_at_capacity_duration_ms, 0)) AS waiting_at_capacity_ms,
  SUM(COALESCE(qh.compilation_duration_ms, 0)) AS compilation_duration_ms,
  SUM(COALESCE(qh.total_task_duration_ms, 0)) AS total_task_duration_ms,
  ROUND(SUM(COALESCE(qh.total_duration_ms, 0)) / 1000.0, 2) AS total_duration_sec,
  ROUND(SUM(COALESCE(qh.execution_duration_ms, 0)) / 1000.0, 2) AS execution_duration_sec,
  ROUND(AVG(qh.total_duration_ms), 1) AS avg_total_duration_ms,
  ROUND(PERCENTILE(qh.total_duration_ms, 0.95), 1) AS p95_total_duration_ms,
  SUM(COALESCE(qh.read_bytes, 0)) AS bytes_scanned,
  SUM(COALESCE(qh.read_rows, 0)) AS read_rows,
  SUM(CASE WHEN qh.execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries,
  SUM(CASE WHEN qh.from_result_cache THEN 1 ELSE 0 END) AS cached_queries
FROM system.query.history qh
WHERE qh.start_time >= TIMESTAMP '{{start_ts}}'
  AND qh.start_time <  TIMESTAMP '{{end_ts}}'
  AND qh.query_source.genie_space_id IN ({{space_id_list}})
GROUP BY ALL
ORDER BY execution_duration_ms DESC;
