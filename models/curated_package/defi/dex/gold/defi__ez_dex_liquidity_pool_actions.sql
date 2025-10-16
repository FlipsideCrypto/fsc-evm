{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE','platform'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, pool_address, token_address)",
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEX, LIQUIDITY, POOLS, LP, SWAPS',
    } } },
    tags = ['gold','defi','dex','lp_actions','curated','ez']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        liquidity_provider,
        sender,
        receiver,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        amounts_unadj,
        amounts,
        amounts_usd,
        tokens_is_verified,
        platform,
        protocol,
        version AS protocol_version,
        modified_timestamp
    FROM
        {{ ref('silver_dex__complete_dex_liquidity_pool_actions') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }}
    )
{% endif %}
),
flattened_tokens AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        liquidity_provider,
        sender,
        receiver,
        pool_address,
        pool_name,
        tokens,
        symbols,
        decimals,
        amounts_unadj,
        amounts,
        amounts_usd,
        tokens_is_verified,
        platform,
        protocol,
        protocol_version,
        modified_timestamp,
        f.key AS token_key,
        f.value :: STRING AS token_address
    FROM
        base,
        LATERAL FLATTEN(
            input => tokens
        ) f
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    liquidity_provider,
    sender,
    receiver,
    pool_address,
    pool_name,
    token_address,
    symbols [token_key] :: STRING AS symbol,
    decimals [token_key] :: INT AS decimals,
    amounts_unadj [token_key] :: FLOAT AS amount_unadj,
    amounts [token_key] :: FLOAT AS amount,
    IFF(
        amount = 0,
        0,
        amounts_usd [token_key] :: FLOAT
    ) AS amount_usd,
    IFNULL(
        tokens_is_verified [token_key] :: BOOLEAN,
        FALSE
    ) AS token_is_verified,
    platform,
    protocol,
    protocol_version,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index','token_key']
    ) }} AS ez_dex_liquidity_pool_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    flattened_tokens
WHERE
    token_address IS NOT NULL
