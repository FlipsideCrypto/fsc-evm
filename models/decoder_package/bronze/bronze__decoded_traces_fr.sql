{{ config (
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ ref('bronze__decoded_traces_fr_v2') }}
UNION ALL
SELECT
    *
FROM
    {{ ref('bronze__decoded_traces_fr_v1') }}