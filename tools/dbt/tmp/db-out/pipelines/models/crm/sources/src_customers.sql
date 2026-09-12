
-- Deduplicate raw customer records, keeping the latest row per customer_id.
-- After editing this query, you MUST run `dbt run --full-refresh` to deploy the change.
-- Schema-drift detection only checks columns, types, and WITH options — query logic
-- changes are not detected and will be silently skipped on a normal `dbt run`.
{{ config(
    materialized = 'streaming_table',
    with= {
        'changelog.mode': 'retract',
        'connector': 'confluent',
        'kafka.cleanup-policy': 'compact',
        'kafka.compaction.time': '0 ms',
        'kafka.max-message-size': '2097164 bytes',
        'scan.bounded.mode': 'unbounded',
        'scan.startup.mode': 'earliest-offset',
        'value.format': 'avro-registry'
    }
) }}
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    created_at,
    updated_at
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC
        ) AS row_num
    FROM {{ source('crm', 'raw_customers') }}
)
WHERE row_num = 1;
