{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'token_address',
    tags = ['silver_stablecoins','defi','stablecoins','curated']
) }}

SELECT
    s.token_address,
    UPPER(COALESCE(s.symbol, m.symbol)) AS symbol,
    COALESCE(
        s.name,
        m.name
    ) AS NAME,
    m.decimals,
    s.blockchain,
    s.peg_type,
    s.peg_mechanism,
    s.inserted_timestamp,
    s.modified_timestamp,
    s.tokens_stablecoins_id AS stablecoins_metadata_id
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
    m.is_verified --verified tokens only
