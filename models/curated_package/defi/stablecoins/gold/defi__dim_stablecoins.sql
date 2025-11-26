{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'contract_address',
    post_hook = '{{ unverify_stablecoins() }}',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['gold','defi','stablecoins','heal','curated_daily']
) }}

WITH crosschain_stablecoins AS (

    SELECT
        s.token_address AS contract_address,
        UPPER(COALESCE(s.symbol, m.symbol)) AS symbol,
        COALESCE(
            s.name,
            m.name
        ) AS NAME,
        m.decimals,
        m.is_verified,
        m.is_verified_modified_timestamp
    FROM
        {{ source(
            'crosschain_silver',
            'tokens_stablecoins'
        ) }}
        s
        INNER JOIN {{ ref('price__ez_asset_metadata') }}
        m
        ON s.token_address = m.token_address
        AND s.blockchain = m.blockchain
    WHERE
        m.is_verified --verified stablecoins only

{% if is_incremental() %}
AND s.modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
manual_stablecoins AS (
    SELECT
        s.contract_address,
        UPPER(
            m.symbol
        ) AS symbol,
        m.name,
        m.decimals,
        m.is_verified,
        m.is_verified_modified_timestamp
    FROM
        {{ ref('silver_stablecoins__stablecoins_mapping_seed') }}
        s
        INNER JOIN {{ ref('price__ez_asset_metadata') }}
        m
        ON s.contract_address = m.token_address
        AND s.blockchain = m.blockchain
    WHERE
        m.is_verified --verified stablecoins only

{% if is_incremental() %}
AND s.contract_address NOT IN (
    SELECT
        contract_address
    FROM
        {{ this }}
)
{% endif %}
),
all_stablecoins AS (
    SELECT
        *
    FROM
        crosschain_stablecoins
    UNION ALL
    SELECT
        *
    FROM
        manual_stablecoins
),

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
heal_model AS (
    SELECT
        t.contract_address,
        m.symbol AS symbol_heal,
        m.name AS name_heal,
        CONCAT(
            m.symbol,
            ': ',
            m.name
        ) AS label_heal,
        m.decimals AS decimals_heal,
        m.is_verified AS is_verified_heal,
        m.is_verified_modified_timestamp AS is_verified_modified_timestamp_heal
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('price__ez_asset_metadata') }}
        m
        ON t.contract_address = m.token_address
        AND t.blockchain = m.blockchain
    WHERE
        t.symbol IS NULL
        OR t.name IS NULL
        OR t.decimals IS NULL
),
{% endif %}

FINAL AS (
    SELECT
        contract_address,
        symbol,
        NAME,
        CONCAT(
            symbol,
            ': ',
            NAME
        ) AS label,
        decimals,
        is_verified,
        is_verified_modified_timestamp
    FROM
        all_stablecoins

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
UNION ALL
SELECT
    contract_address,
    symbol_heal AS symbol,
    name_heal AS NAME,
    label_heal AS label,
    decimals_heal AS decimals,
    is_verified_heal AS is_verified,
    is_verified_modified_timestamp_heal AS is_verified_modified_timestamp
FROM
    heal_model
{% endif %}
)
SELECT
    contract_address,
    symbol,
    NAME,
    label,
    decimals,
    is_verified,
    is_verified_modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['contract_address']) }} AS dim_stablecoins_id
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY dim_stablecoins_id
ORDER BY
    modified_timestamp DESC)) = 1
