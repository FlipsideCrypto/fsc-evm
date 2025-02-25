{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'ephemeral'
) }}

{% set new_build = var(
    'TRACES_REALTIME_NEW_BUILD',
    false
) %}

{% if new_build %}

    SELECT
        -1 AS block_number
    {% else %}
    SELECT
        DISTINCT tx.block_number
    FROM
        {{ ref("test_gold__fact_transactions_recent") }}
        tx
        LEFT JOIN {{ ref("test_gold__fact_traces_recent") }}
        tr USING (
            block_number,
            tx_hash
        )
    WHERE
        tr.tx_hash IS NULL
        AND tx.block_timestamp > DATEADD('day', -5, SYSDATE())
{% endif %}