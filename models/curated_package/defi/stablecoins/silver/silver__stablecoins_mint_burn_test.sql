{# Get variables #}
{% set vars = return_vars() %}
{# Log configuration details #}
{{ log_model_details() }}
-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["stablecoins_mint_burn_locked_id"],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = '{{ unverify_stablecoins() }}',
    tags = ['gold','defi','stablecoins','heal','curated']
) }}

WITH contract_mapping AS (
    {{ curated_contract_mapping(
        vars.CURATED_DEFI_STABLECOINS_BRIDGE_VAULT_MAPPING
    ) }}
),
bridge_vaults AS (
    SELECT
        contract_address AS vault_address,
        CONCAT(
            protocol,
            '-',
            version
        ) AS platform,
        TYPE AS token_address
    FROM
        contract_mapping
),
verified_stablecoins AS (
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
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            to_address,
            contract_address
            ORDER BY
                event_index ASC
        ) AS row_num,
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
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            to_address,
            contract_address
            ORDER BY
                event_index ASC
        ) AS row_num,
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
bridged_out_txs_raw AS (
    SELECT
        tx_hash,
        event_index,
        COALESCE(
            vault_address,
            bridge_address
        ) AS to_address,
        platform,
        token_address AS contract_address,
        amount AS bridge_amount,
        amount_unadj AS bridge_amount_unadj,
        amount_usd AS bridge_amount_usd,
    FROM
        {{ ref('defi__ez_bridge_activity') }}
        LEFT JOIN bridge_vaults USING (
            platform,
            token_address
        )
    WHERE
        platform NOT IN (
            '{{ vars.CURATED_DEFI_STABLECOINS_NATIVE_MINT_BURN_BRIDGE_LIST | join("',
            '") }}'
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
bridged_out_txs AS (
    SELECT
        tx_hash,
        to_address,
        platform,
        contract_address,
        bridge_amount,
        bridge_amount_unadj,
        bridge_amount_usd,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            to_address,
            contract_address
            ORDER BY
                event_index ASC
        ) AS row_num
    FROM
        bridged_out_txs_raw
),
locked_tokens AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        contract_address,
        symbol,
        NAME,
        decimals,
        from_address,
        to_address,
        bridge_amount,
        bridge_amount_unadj,
        (
            amount - bridge_amount
        ) / bridge_amount * 100 AS amount_diff_percent,
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        all_transfers
        INNER JOIN bridged_out_txs USING (
            tx_hash,
            to_address,
            contract_address,
            row_num
        )
    WHERE
        bridge_amount_unadj > 0
        AND from_address != '0x0000000000000000000000000000000000000000'
        AND to_address != '0x0000000000000000000000000000000000000000'
),
unlock_tokens AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        contract_address,
        symbol,
        NAME,
        decimals,
        from_address,
        to_address,
        amount,
        amount_raw,
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        all_transfers
    WHERE
        from_address IN (
            SELECT
                to_address
            FROM
                bridged_out_txs
        )
        AND from_address != '0x0000000000000000000000000000000000000000'
        AND to_address != '0x0000000000000000000000000000000000000000'
),
all_transfer_types AS (
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
        -- amount_raw_precise,
        amount_raw,
        --amount_precise,
        amount,
        0 AS amount_diff_percent,
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
        --amount_raw_precise,
        amount_raw,
        --amount_precise,
        amount,
        0 AS amount_diff_percent,
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        all_transfers
    WHERE
        to_address = '0x0000000000000000000000000000000000000000'
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'Lock' AS event_name,
        contract_address,
        symbol,
        NAME,
        decimals,
        from_address,
        to_address,
        bridge_amount_unadj,
        bridge_amount,
        amount_diff_percent,
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        locked_tokens
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'Unlock' AS event_name,
        contract_address,
        symbol,
        NAME,
        decimals,
        from_address,
        to_address,
        amount_raw,
        amount,
        0 AS amount_diff_percent,
        tx_succeeded,
        _log_id,
        modified_timestamp
    FROM
        unlock_tokens
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
    symbol,
    NAME,
    decimals,
    from_address,
    to_address,
    --amount_raw_precise,
    amount_raw,
    --amount_precise,
    amount,
    amount_diff_percent,
    tx_succeeded,
    _log_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }} AS stablecoins_mint_burn_locked_id
FROM
    all_transfer_types
