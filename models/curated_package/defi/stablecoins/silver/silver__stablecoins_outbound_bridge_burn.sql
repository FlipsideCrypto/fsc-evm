{# {{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE']
) }}

WITH bridge_txs AS (

    SELECT
        tx_hash,
        event_index AS bridge_event_index,
        bridge_address,
        platform,
        token_address,
        token_symbol,
        amount AS bridge_amount,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            bridge_address,
            token_address
            ORDER BY
                event_index ASC
        ) AS rn
    FROM
        {{ ref('defi__ez_bridge_activity') }}
    WHERE
        block_timestamp :: DATE >= '2022-01-01'
        AND platform NOT IN (
            'circle_cctp-v2',
            'chainlink_ccip-v1',
            'stargate-v1',
            'circle_cctp-v1',
            'allbridge-v2',
            'starknet_starkgate_eth_bridge-v1',
            'everclear-v1',
            'multichain-v7',
            'stargate-v2',
            'zora_bridge-v1',
            'arbitrum_nova_bridge-v1',
            'layerzero-v2'
        )
),
transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index AS transfer_event_index,
        from_address,
        to_address AS bridge_address,
        contract_address AS token_address,
        amount,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            to_address,
            contract_address
            ORDER BY
                event_index ASC
        ) AS rn
    FROM
        {{ ref('core__ez_token_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2022-01-01'
),
stablecoins AS (
    SELECT
        token_address,
        NAME
    FROM
        {{ ref('defi__dim_stablecoins') }}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    bridge_event_index,
    transfer_event_index,
    bridge_address,
    platform,
    token_address,
    token_symbol,
    NAME,
    amount,
    bridge_amount,
    (
        amount - bridge_amount
    ) / bridge_amount * 100 AS amount_diff_percent
FROM
    transfers
    INNER JOIN bridge_txs USING (
        tx_hash,
        bridge_address,
        token_address,
        rn
    )
    INNER JOIN stablecoins USING (token_address)
WHERE
    amount > 0 #}
