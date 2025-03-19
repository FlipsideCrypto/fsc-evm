{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = "view",
    tags = get_path_tags(model)
) }}

SELECT
    *
FROM
    {{ ref('silver__receipts') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref('_block_lookback') }}
    )
