{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'morpho_blue_v1_reads_id',
    tags = ['silver','contract_reads']
) }}

WITH market_tokens AS (
    -- Get unique tokens from CreateMarket events

    SELECT
        DISTINCT token_address
    FROM
        (
            -- Collateral tokens
            SELECT
                LOWER(
                    decoded_log :marketParams :collateralToken :: STRING
                ) AS token_address
            FROM
                {{ ref('core__ez_decoded_event_logs') }}
            WHERE
                contract_address = '{{ vars.CURATED_DEFI_TVL_MORPHO_BLUE_ADDRESS }}'
                AND event_name = 'CreateMarket'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
UNION
    -- Loan tokens
SELECT
    LOWER(
        decoded_log :marketParams :loanToken :: STRING
    ) AS token_address
FROM
    {{ ref('core__ez_decoded_event_logs') }}
WHERE
    contract_address = '{{ vars.CURATED_DEFI_TVL_MORPHO_BLUE_ADDRESS }}'
    AND event_name = 'CreateMarket'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
WHERE
    token_address <> '0x0000000000000000000000000000000000000000'
    AND token_address IS NOT NULL
)
SELECT
    token_address AS contract_address,
    '{{ vars.CURATED_DEFI_TVL_MORPHO_BLUE_ADDRESS }}' AS address,
    'balanceOf' AS function_name,
    '0x70a08231' AS function_sig,
    CONCAT(
        '0x70a08231',
        LPAD(
            SUBSTR(
                '{{ vars.CURATED_DEFI_TVL_MORPHO_BLUE_ADDRESS }}',
                3
            ),
            64,
            '0'
        )
    ) AS input,
    NULL::VARIANT AS metadata,
    'morpho' AS protocol,
    'v1' AS version,
    CONCAT(
        protocol,
        '-',
        version
    ) AS platform,
    {{ dbt_utils.generate_surrogate_key(['contract_address','address','input','platform']) }} AS morpho_blue_v1_reads_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    market_tokens
