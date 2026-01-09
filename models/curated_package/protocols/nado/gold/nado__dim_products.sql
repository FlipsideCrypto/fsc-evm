{# Log configuration details #}
{{ log_model_details() }}

 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'NADO',
                'PURPOSE': 'CLOB, DEX, PRODUCTS'
            }
        }
    },
    tags = ['gold','nado','curated']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    product_id,
    product_type,
    ticker_id,
    symbol,
    name,
    nado_products_id AS dim_products_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nado_dim_products') }}
ORDER BY product_id