{{ config(
    materialized='streaming_table',
    distributed_by='tenant_id,group_id',
    with={
        'changelog.mode': 'upsert',
        'key.format': 'avro-registry',
        'value.format': 'avro-registry',
        'kafka.retention.time': '0',
        'kafka.producer.compression.type': 'snappy',
        'scan.bounded.mode': 'unbounded',
        'scan.startup.mode': 'earliest-offset',
        'value.fields-include': 'all'
    }
) }}

-- Migrated from dml.c360_fct_user_per_group.sql
select
    tenant_id,
    group_id,
    group_name,
    COUNT(*) as total_users,
    SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) as active_users,
    SUM(CASE WHEN is_active = false THEN 1 ELSE 0 END) as inactive_users
from {{ ref('sl_c360_dim_users') }}
group by tenant_id, group_id, group_name, is_active
