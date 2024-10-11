{{ config (
    materialized = 'view',
    tags = ['decoder']
) }}

SELECT
    *
FROM
    {{ ref('bronze__decoded_logs_fr_v2') }}
UNION ALL
SELECT
    *
FROM
    {{ ref('bronze__decoded_logs_fr_v1') }}