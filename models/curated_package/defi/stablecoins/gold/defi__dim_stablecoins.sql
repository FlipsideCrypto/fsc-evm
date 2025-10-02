{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STABLECOINS',
    } } },
    tags = ['gold','defi','stablecoins','curated']
) }}

SELECT
    token_address,
    symbol,
    NAME,
    decimals,
    peg_type,
    peg_mechanism,
    inserted_timestamp,
    modified_timestamp,
    stablecoins_metadata_id AS dim_stablecoins_id
FROM
    {{ ref('silver_stablecoins__stablecoins_metadata') }}
