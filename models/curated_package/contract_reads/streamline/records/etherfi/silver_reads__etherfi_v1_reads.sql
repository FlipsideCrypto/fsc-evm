{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'etherfi_v1_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH contracts AS (

    SELECT
        contract_address,
        address,
        token_address,
        function_name,
        function_sig,
        chain,
        attribution
    FROM
        {{ ref('silver_reads__etherfi_seed') }}
    WHERE
        chain = '{{ vars.GLOBAL_PROJECT_NAME }}'

{% if is_incremental() %}
AND
    CONCAT(contract_address, '-', COALESCE(address, 'null')) NOT IN (
        SELECT
            CONCAT(contract_address, '-', COALESCE(address, 'null'))
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
    CASE
        WHEN function_name = 'balanceOf' THEN CONCAT(
            '0x70a08231',
            LPAD(SUBSTR(address, 3), 64, '0')
        )
        ELSE RPAD(
            function_sig,
            64,
            '0'
        )
    END AS input,
    OBJECT_CONSTRUCT(
        'token_address', token_address,
        'attribution', attribution,
        'chain', chain
    ) :: variant AS metadata,
    'etherfi' AS protocol,
    'v1' AS version,
    CONCAT(
        protocol,
        '-',
        version
    ) AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address','address','input','platform']
    ) }} AS etherfi_v1_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    contracts
