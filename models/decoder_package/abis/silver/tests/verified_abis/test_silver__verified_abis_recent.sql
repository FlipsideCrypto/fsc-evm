{# Log configuration details #}
{{ log_model_details() }}

{% set decoder_abis_block_explorer_name = var('DECODER_ABIS_BLOCK_EXPLORER_NAME', '') %}

{{ config (
    materialized = "view",
    tags = ['daily_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__verified_abis') }}
WHERE
    _inserted_timestamp > DATEADD(DAY, -5, SYSDATE())
    and abi_source = LOWER('{{ decoder_abis_block_explorer_name }}') 