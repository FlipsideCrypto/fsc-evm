{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','erc4626','curated','maple']
) }}

WITH pools AS (
    SELECT
        vault_address,
        vault_name,
        vault_symbol,
        vault_decimals,
        underlying_asset_address,
        underlying_symbol,
        underlying_decimals,
        protocol,
        version,
        platform
    FROM
        {{ ref('silver_erc4626__maple_pools') }}
),

{# ERC4626 Deposit event: Deposit(address indexed sender, address indexed owner, uint256 assets, uint256 shares) #}
deposit_events AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.event_index,
        l.origin_from_address,
        l.origin_to_address,
        l.origin_function_signature,
        l.contract_address AS vault_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics[1]::STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics[2]::STRING, 27, 40)) AS owner,
        utils.udf_hex_to_int(segmented_data[0]::STRING)::NUMERIC AS assets_raw,
        utils.udf_hex_to_int(segmented_data[1]::STRING)::NUMERIC AS shares_raw,
        l.modified_timestamp,
        CONCAT(l.tx_hash::STRING, '-', l.event_index::STRING) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }} l
    WHERE
        l.topics[0]::STRING = '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'
        AND l.contract_address IN (SELECT vault_address FROM pools)
        AND l.tx_succeeded

{% if is_incremental() %}
    AND l.modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '{{ vars.CURATED_LOOKBACK_HOURS }}'
        FROM
            {{ this }}
    )
    AND l.modified_timestamp >= SYSDATE() - INTERVAL '{{ vars.CURATED_LOOKBACK_DAYS }}'
{% endif %}
)

SELECT
    d.tx_hash,
    d.block_number,
    d.block_timestamp,
    d.event_index,
    d.origin_from_address,
    d.origin_to_address,
    d.origin_function_signature,
    d.vault_address,
    d.sender,
    d.owner AS depositor,
    d.assets_raw AS amount_unadj,
    d.shares_raw AS shares_unadj,
    p.vault_address AS protocol_market,
    p.vault_symbol AS protocol_market_symbol,
    p.underlying_asset_address AS token_address,
    p.underlying_symbol AS token_symbol,
    p.underlying_decimals AS token_decimals,
    p.protocol,
    p.version,
    p.platform,
    d._log_id,
    d.modified_timestamp,
    'Deposit' AS event_name
FROM
    deposit_events d
LEFT JOIN pools p
    ON d.vault_address = p.vault_address
QUALIFY(ROW_NUMBER() OVER (PARTITION BY d._log_id ORDER BY d.modified_timestamp DESC)) = 1
