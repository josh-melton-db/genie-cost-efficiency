-- AI Gateway token intensity (account admin)
SELECT
  DATE(request_time) AS usage_date,
  requester,
  endpoint_name,
  SUM(input_tokens) AS input_tokens,
  SUM(output_tokens) AS output_tokens,
  SUM(total_tokens) AS total_tokens,
  AVG(request_duration_ms) AS avg_latency_ms
FROM system.ai_gateway.usage
WHERE request_time >= TIMESTAMP '{{start_ts}}'
  AND request_time <  TIMESTAMP '{{end_ts}}'
GROUP BY ALL
ORDER BY total_tokens DESC;
