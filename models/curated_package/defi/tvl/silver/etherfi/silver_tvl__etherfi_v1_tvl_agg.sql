{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'etherfi_v1_tvl_agg_id',
    tags = ['silver','defi','tvl','curated_daily']
) }}

WITH chain_tvl AS (

    SELECT
        block_number,
        block_date,
        contract_address,
        address,
        token_address,
        amount_hex,
        amount_raw,
        protocol,
        version,
        platform,
        attribution,
        chain
    FROM
        {{ ref('silver_tvl__etherfi_v1_tvl') }}
    WHERE
        chain = '{{ vars.GLOBAL_PROJECT_NAME }}'
        AND attribution = '{{ vars.GLOBAL_PROJECT_NAME }}'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
), {% if vars.GLOBAL_PROJECT_NAME == 'ethereum' %}
    eth_attributed_tvl AS (
        SELECT
            block_number,
            block_date,
            contract_address,
            address,
            token_address,
            amount_hex,
            amount_raw,
            protocol,
            version,
            platform,
            attribution,
            chain
        FROM
            {{ source(
                'silver_tvl_optimism',
                'etherfi_v1_tvl'
            ) }}
        WHERE
            attribution = 'ethereum'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
UNION ALL
SELECT
    block_number,
    block_date,
    contract_address,
    address,
    token_address,
    amount_hex,
    amount_raw,
    protocol,
    version,
    platform,
    attribution,
    chain
FROM
    {{ source(
        'silver_tvl_arbitrum',
        'etherfi_v1_tvl'
    ) }}
WHERE
    attribution = 'ethereum'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
UNION ALL
SELECT
    block_number,
    block_date,
    contract_address,
    address,
    token_address,
    amount_hex,
    amount_raw,
    protocol,
    version,
    platform,
    attribution,
    chain
FROM
    {{ source(
        'silver_tvl_base',
        'etherfi_v1_tvl'
    ) }}
WHERE
    attribution = 'ethereum'

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
{% endif %}

FINAL AS (
    SELECT
        *
    FROM
        chain_tvl

    {% if vars.GLOBAL_PROJECT_NAME == 'ethereum' %}
    UNION ALL
    SELECT
        *
    FROM
        eth_attributed_tvl
    {% endif %}
)
SELECT
    block_number,
    block_date,
    contract_address,
    address,
    token_address,
    amount_hex,
    amount_raw,
    protocol,
    version,
    platform,
    attribution,
    chain,
    {{ dbt_utils.generate_surrogate_key(
        ['block_date','contract_address','address','platform','attribution','chain']
    ) }} AS etherfi_v1_tvl_agg_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY etherfi_v1_tvl_agg_id
ORDER BY
    modified_timestamp DESC)) = 1
