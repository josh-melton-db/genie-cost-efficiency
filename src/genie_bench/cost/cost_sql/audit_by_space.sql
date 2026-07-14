-- Audit activity by Genie space_id
SELECT
  request_params.space_id AS space_id,
  action_name,
  COUNT(*) AS events,
  COUNT(DISTINCT user_identity.email) AS users
FROM system.access.audit
WHERE service_name = 'aibiGenie'
  AND action_name IN (
    'createConversationMessage',
    'regenerateConversationMessage',
    'genieCreateConversationMessage',
    'genieStartConversationMessage'
  )
  AND event_time >= TIMESTAMP '{{start_ts}}'
  AND event_time <  TIMESTAMP '{{end_ts}}'
  AND request_params.space_id IN ({{space_id_list}})
GROUP BY 1, 2
ORDER BY events DESC;
