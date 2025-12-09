{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'tornado_cash_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH mixers AS (

    SELECT
        token_address AS contract_address,
        mixer_address AS address,
        'balanceOf' AS function_name,
        '0x70a08231' AS function_sig,
        CONCAT(
            '0x70a08231',
            LPAD(SUBSTR(address, 3), 64, '0')
        ) AS input,
    FROM
        {{ ref('silver_reads__tornado_cash_mixer_seed') }}
    WHERE
        chain = '{{ vars.GLOBAL_PROJECT_NAME }}'
        AND contract_address IS NOT NULL --balanceOf calls only apply to erc20 token-mixer pairs. eth_getBalance calls to be handled downstream for null-mixer pairs.

{% if is_incremental() %}
AND CONCAT(COALESCE(contract_address, 'null'), '-', address) NOT IN (
    SELECT
        CONCAT(COALESCE(contract_address, 'null'), '-', address)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    contract_address,
    address,
    function_name,
    function_sig,
    input,
    NULL :: variant AS metadata,
    'tornado_cash' AS protocol,
    'v1' AS version,
    CONCAT(
        protocol,
        '-',
        version
    ) AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','address','input','platform']
    ) }} AS tornado_cash_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    mixers
