{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = "incremental",
    unique_key = "balances_erc20_daily_history_records_id",
    cluster_by = "ROUND(block_number, -3)",
    post_hook = '{{ unverify_balances() }}',
    full_refresh = vars.GLOBAL_STREAMLINE_FR_ENABLED,
    tags = ['streamline','balances','history','erc20','heal','phase_4']
) }}

WITH last_x_days AS (

    SELECT
        block_number,
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
    WHERE block_number >= {{ vars.BALANCES_SL_START_BLOCK }}
),
verified_contracts AS (
    SELECT
        DISTINCT token_address
    FROM
        {{ ref('price__ez_asset_metadata') }}
    WHERE
        is_verified
        AND token_address IS NOT NULL
),
{% if is_incremental() and var('HEAL_MODEL',false) %}
newly_verified_contracts AS (
    SELECT DISTINCT token_address AS contract_address
    FROM {{ ref('price__ez_asset_metadata') }}
    WHERE token_address NOT IN (
        SELECT DISTINCT contract_address 
        FROM {{ this }})
    AND is_verified
    AND token_address IS NOT NULL
),
newly_verified_logs AS (
    SELECT
        block_number,
        block_timestamp,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS address1,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 42)) AS address2
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        (
            topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            OR (
                topics [0] :: STRING = '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65'
                AND contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
            )
            OR (
                topics [0] :: STRING = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
                AND contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
            )
        )
        AND block_number < (
            SELECT MAX(block_number)
            FROM last_x_days
        ) 
        AND block_number >= {{ vars.BALANCES_SL_START_BLOCK }} 
        --only include events prior to 1 day ago
        AND contract_address IN (
            SELECT
                token_address
            FROM
                newly_verified_contracts
        )
),
{% endif %}
logs AS (
    SELECT
        block_number,
        block_timestamp,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS address1,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 42)) AS address2
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        (
            topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            OR (
                topics [0] :: STRING = '0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65'
                AND contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
            )
            OR (
                topics [0] :: STRING = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
                AND contract_address = '{{ vars.GLOBAL_WRAPPED_NATIVE_ASSET_ADDRESS }}'
            )
        )
        AND block_number < (
            SELECT MAX(block_number)
            FROM last_x_days
        ) 
        AND block_number >= {{ vars.BALANCES_SL_START_BLOCK }} 
        --only include events prior to 1 day ago
        AND contract_address IN (
            SELECT
                token_address
            FROM
                verified_contracts
        )
    {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '72 hours'
            FROM {{ this }}
        )
    {% endif %}
),
all_logs AS (
    SELECT *
    FROM logs
    {% if is_incremental() and var('HEAL_MODEL',false) %}
    UNION
    SELECT *
    FROM newly_verified_logs
    {% endif %}
),
transfers AS (
    SELECT
        DISTINCT 
        block_timestamp :: DATE AS block_date,
        contract_address,
        address1 AS address
    FROM
        all_logs
    WHERE
        address1 IS NOT NULL
        AND address1 <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT 
        block_timestamp :: DATE AS block_date,
        contract_address,
        address2 AS address
    FROM
        all_logs
    WHERE
        address2 IS NOT NULL
        AND address2 <> '0x0000000000000000000000000000000000000000'
),
to_do AS (
    SELECT
        DISTINCT
        d.block_number,
        d.block_date,
        t.address,
        t.contract_address
    FROM
        transfers t
        INNER JOIN last_x_days d 
            ON t.block_date = d.block_date
    --max daily block_number during the selected period, for each contract_address/address pair
    WHERE
        t.block_date IS NOT NULL
        AND d.block_number < (
            SELECT MAX(block_number)
            FROM last_x_days
        ) 
)
SELECT
    block_number,
    block_date,
    address,
    contract_address,
    {{ dbt_utils.generate_surrogate_key(['block_date', 'address', 'contract_address']) }} AS balances_erc20_daily_history_records_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    to_do qualify (ROW_NUMBER() over (PARTITION BY balances_erc20_daily_history_records_id
ORDER BY
    modified_timestamp DESC)) = 1