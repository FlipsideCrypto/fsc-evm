{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    post_hook = '{{ unverify_stablecoins() }}',
    tags = ['silver','defi','stablecoins','heal','curated']
) }}


WITH verified_stablecoins AS (

    SELECT
        contract_address,
        decimals,
        symbol,
        NAME
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        is_verified
        AND contract_address IS NOT NULL
),

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
newly_verified_stablecoins AS (
    SELECT
        contract_address,
        decimals,
        symbol,
        NAME
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        IFNULL(
            is_verified_modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ) > DATEADD(
            'day',
            -8,
            (
                SELECT
                    MAX(modified_timestamp) :: DATE
                FROM
                    {{ this }}
            )
        )
),
newly_verified_blacklist AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        CASE
            WHEN topic_0 = '0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc' THEN 'AddedBlacklist'
            ELSE 'RemovedBlacklist'
        END AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(SUBSTR(DATA, 3, 64), 25, 40)) :: STRING AS user_address,
        s.decimals,
        s.symbol,
        s.name,
        tx_succeeded,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN newly_verified_stablecoins s
        ON l.contract_address = s.contract_address
    WHERE
        topic_0 :: STRING IN ('0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc', --AddedBlacklist
        '0xd7e9ec6e6ecd65492dce6bf513cd6867560d49544421d0783ddf06e76c24470c') --RemovedBlacklist
),
{% endif %}

blacklist AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        CASE
            WHEN topic_0 = '0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc' THEN 'AddedBlacklist'
            ELSE 'RemovedBlacklist'
        END AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(SUBSTR(DATA, 3, 64), 25, 40)) :: STRING AS user_address,
        s.decimals,
        s.symbol,
        s.name,
        tx_succeeded,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        INNER JOIN verified_stablecoins s
        ON l.contract_address = s.contract_address
    WHERE
        topic_0 :: STRING IN ('0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc', --AddedBlacklist
        '0xd7e9ec6e6ecd65492dce6bf513cd6867560d49544421d0783ddf06e76c24470c') --RemovedBlacklist

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
all_blacklist AS (
    SELECT
        *
    FROM
        blacklist

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
UNION
SELECT
    *
FROM
    newly_verified_blacklist
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    symbol,
    NAME,
    decimals,
    user_address,
    tx_succeeded,
    _log_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }} AS stablecoins_address_blacklist_id
FROM
    all_blacklist