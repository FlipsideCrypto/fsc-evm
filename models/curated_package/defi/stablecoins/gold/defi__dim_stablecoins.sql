{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'token_address',
    post_hook = '{{ unverify_stablecoins() }}',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STABLECOINS',
    } } },
    tags = ['gold','defi','stablecoins','heal','curated']
) }}

SELECT
    s.token_address,
    UPPER(COALESCE(s.symbol, m.symbol)) AS symbol,
    COALESCE(
        s.name,
        m.name
    ) AS NAME,
    m.decimals,
    s.peg_type,
    s.peg_mechanism,
    m.is_verified,
    m.is_verified_modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['s.token_address']) }} AS dim_stablecoins_id
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
