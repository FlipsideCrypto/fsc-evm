{#
    Chainlink Protocol Macros
    These macros generate SQL for Chainlink protocol metrics models
    Updated to support incremental builds with modified_timestamp
#}

{% macro chainlink_logs(chain, topic_hashes, is_incremental_run=false, lookback_hours='48', lookback_days='7') %}
{#
    Generates SQL for querying Chainlink event logs by topic hashes
    Args:
        chain: The blockchain name (e.g., 'ethereum', 'polygon')
        topic_hashes: Tuple of event topic hashes to filter
        is_incremental_run: Whether this is an incremental run
        lookback_hours: Hours to look back for incremental
        lookback_days: Days to look back for incremental safety
#}
WITH base_logs AS (
    SELECT
        block_number
        , block_timestamp
        , tx_hash
        , event_index
        , contract_address
        , topics
        , data
        , origin_from_address
        , origin_to_address
        , origin_function_signature
        , modified_timestamp
        , CONCAT(tx_hash, '-', event_index) AS _log_id
    FROM {{ ref('core__fact_event_logs') }}
    WHERE topics[0]::string IN {{ topic_hashes }}
    {% if is_incremental_run %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ lookback_hours }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ lookback_days }} days'
    {% endif %}
)
SELECT
    block_number
    , block_timestamp
    , tx_hash
    , event_index
    , contract_address
    , topics
    , data
    , origin_from_address
    , origin_to_address
    , origin_function_signature
    , '{{ chain }}' AS chain
    , _log_id
    , modified_timestamp
FROM base_logs
{% endmacro %}


{% macro chainlink_vrf_request_fulfilled_logs(chain, is_incremental_run=false, lookback_hours='48', lookback_days='7') %}
{#
    Generates SQL for Chainlink VRF Request Fulfilled event logs
    VRF (Verifiable Random Function) events for randomness requests
    Args:
        chain: The blockchain name
        is_incremental_run: Whether this is an incremental run
        lookback_hours: Hours to look back for incremental
        lookback_days: Days to look back for incremental safety
#}
{# VRF RandomWordsRequest and RandomWordsFulfilled topic hashes #}
{% set vrf_topics = (
    '0x63373d1c4696214b898952999c9aaec57dac1ee2723cec59bea6888f489a9772',
    '0x7dffc5ae5ee4e2e4df1651cf6ad329a73cebdb728f37ea0187b9b17e036756e4'
) %}
{{ chainlink_logs(chain, vrf_topics, is_incremental_run, lookback_hours, lookback_days) }}
{% endmacro %}


{% macro chainlink_ocr_rewards_daily(chain, is_incremental_run=false, lookback_hours='48', lookback_days='7') %}
{#
    Generates SQL for daily Chainlink OCR (Off-Chain Reporting) rewards
    Aggregates OCR transmission rewards by operator
    Args:
        chain: The blockchain name
        is_incremental_run: Whether this is an incremental run
        lookback_hours: Hours to look back for incremental
        lookback_days: Days to look back for incremental safety
#}
WITH ocr_transmissions AS (
    SELECT
        block_number
        , block_timestamp
        , tx_hash
        , event_index
        , contract_address
        , topics
        , data
        , origin_from_address
        , modified_timestamp
    FROM {{ ref('core__fact_event_logs') }}
    WHERE topics[0]::string = '0xd0d9486a2c673e2a4b57fc82e4c8a556b3e2b82dd5db07e2c04a920ca0f469b6'
    {% if is_incremental_run %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ lookback_hours }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ lookback_days }} days'
    {% endif %}
)
, token_transfers AS (
    SELECT
        block_timestamp::date AS date
        , from_address
        , to_address
        , amount
        , contract_address AS token_address
        , tx_hash
        , modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE tx_hash IN (SELECT DISTINCT tx_hash FROM ocr_transmissions)
    {% if is_incremental_run %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ lookback_hours }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ lookback_days }} days'
    {% endif %}
)
SELECT
    date
    , '{{ chain }}' AS chain
    , 'OCR' AS reward_type
    , to_address AS operator_address
    , token_address
    , SUM(amount) AS reward_amount
    , COUNT(DISTINCT tx_hash) AS tx_count
    , MAX(modified_timestamp) AS modified_timestamp
FROM token_transfers
WHERE to_address != from_address
GROUP BY 1, 2, 3, 4, 5
{% endmacro %}


{% macro chainlink_fm_rewards_daily(chain, is_incremental_run=false, lookback_hours='48', lookback_days='7') %}
{#
    Generates SQL for daily Chainlink Flux Monitor rewards
    Flux Monitor is used for price feed submissions
    Args:
        chain: The blockchain name
        is_incremental_run: Whether this is an incremental run
        lookback_hours: Hours to look back for incremental
        lookback_days: Days to look back for incremental safety
#}
WITH fm_submissions AS (
    SELECT
        block_number
        , block_timestamp
        , tx_hash
        , event_index
        , contract_address
        , topics
        , data
        , origin_from_address
        , modified_timestamp
    FROM {{ ref('core__fact_event_logs') }}
    WHERE topics[0]::string IN (
        '0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f',
        '0xf6a97944f31ea060dfde0566e4167c1a1082551e64b60ecb14d599a9d023d451'
    )
    {% if is_incremental_run %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ lookback_hours }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ lookback_days }} days'
    {% endif %}
)
, token_transfers AS (
    SELECT
        block_timestamp::date AS date
        , from_address
        , to_address
        , amount
        , contract_address AS token_address
        , tx_hash
        , modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE tx_hash IN (SELECT DISTINCT tx_hash FROM fm_submissions)
    {% if is_incremental_run %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ lookback_hours }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ lookback_days }} days'
    {% endif %}
)
SELECT
    date
    , '{{ chain }}' AS chain
    , 'FM' AS reward_type
    , to_address AS operator_address
    , token_address
    , SUM(amount) AS reward_amount
    , COUNT(DISTINCT tx_hash) AS tx_count
    , MAX(modified_timestamp) AS modified_timestamp
FROM token_transfers
WHERE to_address != from_address
GROUP BY 1, 2, 3, 4, 5
{% endmacro %}


{% macro chainlink_direct_operator_rewards_daily(chain, is_incremental_run=false, lookback_hours='48', lookback_days='7') %}
{#
    Generates SQL for daily Chainlink direct operator rewards
    Direct rewards paid to operators for node operations
    Args:
        chain: The blockchain name
        is_incremental_run: Whether this is an incremental run
        lookback_hours: Hours to look back for incremental
        lookback_days: Days to look back for incremental safety
#}
WITH operator_payments AS (
    SELECT
        block_timestamp::date AS date
        , from_address
        , to_address
        , amount
        , contract_address AS token_address
        , tx_hash
        , modified_timestamp
    FROM {{ ref('core__ez_token_transfers') }}
    WHERE origin_function_signature IN (
        '0x4ab0d190',
        '0xfbcafdc9',
        '0xf75f0e7a'
    )
    {% if is_incremental_run %}
    AND modified_timestamp >= (
        SELECT MAX(modified_timestamp) - INTERVAL '{{ lookback_hours }} hours'
        FROM {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '{{ lookback_days }} days'
    {% endif %}
)
SELECT
    date
    , '{{ chain }}' AS chain
    , 'Direct' AS reward_type
    , to_address AS operator_address
    , token_address
    , SUM(amount) AS reward_amount
    , COUNT(DISTINCT tx_hash) AS tx_count
    , MAX(modified_timestamp) AS modified_timestamp
FROM operator_payments
WHERE to_address != from_address
GROUP BY 1, 2, 3, 4, 5
{% endmacro %}


{% macro chainlink_non_circulating_supply_addresses() %}
{#
    Returns a CTE with Chainlink non-circulating supply addresses
    Used for treasury calculations
#}
SELECT address FROM (VALUES
    ('0x75442ac771a7243433e033f3f8eab2631e22938c'),
    ('0xbe6977e08d4479c0a6777539ae0e8fa27be4e9d6'),
    ('0x98c63b7b319dfbdf3d811530f2ab9dfe4983af9d'),
    ('0xdad22a85ef8310ef582b70e4051e1a6e94c17467'),
    ('0x276ccbab54490251d1d92bb738e63d36ae9ab95f'),
    ('0x8c50aeb08ca92a32ddda5ba2b020c1a45ecb6c28'),
    ('0x6c2e043b8d6c01b56e79ceea1a8a14b4c0c6d2c2'),
    ('0xb2684da3f4bb9749c88e71af2c9a585e2c1a6bdb'),
    ('0x188adb14c66dee7813e39b3e0f0e9269aa7190eb'),
    ('0x93ecd4517c1eb7d69c48bf44a41e63b72f0a3839'),
    ('0x31a7a98f6a0c3255d29eb91b42bf578d29f1e03e'),
    ('0xca5f1d5b88d20eba2e95c28f9e77b686e81e5e81')
) AS t(address)
{% endmacro %}
