{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'ephemeral'
) }}

{% set new_build = var(
    'RECEIPTS_REALTIME_NEW_BUILD',
    false
) %}
{% set new_build_by_hash = var(
    'RECEIPTS_BY_HASH_REALTIME_NEW_BUILD',
    false
) %}

{% if new_build or new_build_by_hash %}

    SELECT
        -1 AS block_number
    {% else %}
    SELECT
        DISTINCT block_number
    FROM
        {{ ref("test_gold__fact_transactions_recent") }}
    WHERE
        tx_succeeded IS NULL
{% endif %}