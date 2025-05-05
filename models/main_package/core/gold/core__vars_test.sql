{# Get variables #}
{% set vars = return_vars() %}
{% set rpc_vars = set_dynamic_fields('fact_transactions') %}
{# Log configuration details #}
{{ log_model_details() }}
{{ config (
    materialized = "table"
) }}

SELECT
    block_number,
    {% if rpc_vars.l1FeeScalar %}
        r.receipts_json :l1FeeScalar :: STRING AS l1_fee_scalar
    {% endif %}
FROM
    {{ ref('silver__receipts') }}
WHERE
    (
        block_number = 29707122
        AND partition_key = 29707000
        AND array_index = 55
    )
    OR (
        block_number = 10563895
        AND partition_key = 10560000
        AND array_index = 3
    )
