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
    tags = ['gold','defi','stablecoins','heal','curated']
) }}


WITH verified_stablecoins AS (

    SELECT
        token_address,
        decimals,
        symbol,
        NAME
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        is_verified
        AND token_address IS NOT NULL
),

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
newly_verified_stablecoins AS (
    SELECT
        token_address,
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
newly_verified_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        contract_address,
        CONCAT('0x', SUBSTR(topic_1 :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topic_2 :: STRING, 27, 40)) AS to_address,
        utils.udf_hex_to_int(SUBSTR(DATA, 3, 64)) AS amount_raw_precise,
        amount_raw_precise :: FLOAT AS amount_raw,
        IFF(
            s.decimals IS NULL,
            NULL,
            utils.udf_decimal_adjust(
                amount_raw_precise,
                s.decimals
            )
        ) AS amount_precise,
        amount_precise :: FLOAT AS amount,
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
        ON l.contract_address = s.token_address
    WHERE
        topic_0 :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Transfer
        AND amount_raw IS NOT NULL
),
{% endif %}

transfers AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        contract_address,
        CONCAT('0x', SUBSTR(topic_1 :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topic_2 :: STRING, 27, 40)) AS to_address,
        utils.udf_hex_to_int(SUBSTR(DATA, 3, 64)) AS amount_raw_precise,
        amount_raw_precise :: FLOAT AS amount_raw,
        IFF(
            s.decimals IS NULL,
            NULL,
            utils.udf_decimal_adjust(
                amount_raw_precise,
                s.decimals
            )
        ) AS amount_precise,
        amount_precise :: FLOAT AS amount,
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
        ON l.contract_address = s.token_address
    WHERE
        topic_0 :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Transfer
        AND amount_raw IS NOT NULL

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
all_transfers AS (
    SELECT
        *
    FROM
        transfers

{% if is_incremental() and var(
    'HEAL_MODEL',
    false
) %}
UNION
SELECT
    *
FROM
    newly_verified_transfers
{% endif %}
),
mint_burn AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'Mint' AS event_name,
        contract_address,
        symbol,
        NAME,
        decimals,
        from_address,
        to_address,
        amount_raw_precise,
        amount_raw,
        amount_precise,
        amount,
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        all_transfers
    WHERE
        from_address = '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'Burn' AS event_name,
        contract_address,
        symbol,
        NAME,
        decimals,
        from_address,
        to_address,
        amount_raw_precise,
        amount_raw,
        amount_precise,
        amount,
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        all_transfers
    WHERE
        to_address = '0x0000000000000000000000000000000000000000'
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
    contract_address AS token_address,
    from_address,
    to_address,
    amount_raw_precise,
    amount_raw,
    amount_precise,
    amount,
    tx_succeeded,
    _log_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }} AS stablecoins_mint_burn_id
FROM
    mint_burn