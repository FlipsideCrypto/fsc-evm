{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = ['full_test']
) }}

SELECT
    *
FROM
    {{ ref('nft__ez_nft_transfers') }}