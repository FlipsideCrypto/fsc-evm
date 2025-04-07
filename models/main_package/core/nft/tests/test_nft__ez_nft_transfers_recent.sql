{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['test_nft','nft','recent_test','phase_2']
) }}

SELECT
    *
FROM
    {{ ref('nft__ez_nft_transfers') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref('_block_lookback') }}
    )
