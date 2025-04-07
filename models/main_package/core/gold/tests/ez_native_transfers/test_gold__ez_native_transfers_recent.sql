{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_gold','core','recent_test','transfers','ez','phase_3']
) }}

SELECT
    *
FROM
    {{ ref('core__ez_native_transfers') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref('_block_lookback') }}
    )
