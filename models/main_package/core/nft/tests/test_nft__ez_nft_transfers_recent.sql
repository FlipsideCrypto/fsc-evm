{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['gold','test_nft','nft','core','recent_test']
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
