{{ config(
    materialized = 'table',
    unique_key = ['gem_address', 'join_address'],
    cluster_by = ['gem_address'],
    tags = ['silver_protocols', 'maker', 'gem_join', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

WITH join_addresses AS (
    SELECT
        '0x' || SUBSTR(topics [1], 27) AS join_address,
        *
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] = LOWER(
            '0x65fae35e00000000000000000000000000000000000000000000000000000000'
        )
        AND contract_address = LOWER('0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b')
),

contract_creation_hashes AS (
    SELECT
        address AS join_address,
        created_tx_hash
    FROM
        {{ ref('core__dim_contracts') }}
    WHERE
        address IN (SELECT LOWER(join_address) FROM join_addresses)
)

SELECT
    '0x' || RIGHT(t.input, 40) AS gem_address,
    h.join_address,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    contract_creation_hashes h
LEFT JOIN {{ ref('core__fact_traces') }} t ON h.created_tx_hash = t.tx_hash
HAVING LENGTH(gem_address) = 42
