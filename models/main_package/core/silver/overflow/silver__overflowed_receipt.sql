-- depends_on: {{ ref('bronze__overflowed_receipts') }}
{% set warehouse = 'DBT_SNOWPARK' if var('OVERFLOWED_RECEIPTS') else target.warehouse %}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','tx_position'],
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    tags = ['silver_overflowed_receipts'],
    full_refresh = false,
    snowflake_warehouse = warehouse
) }}

{% if is_incremental() %}
WITH bronze_overflowed_receipts AS (

    SELECT
        block_number :: INT AS block_number,
        ROUND(
            block_number,
            -3
        ) AS partition_key,
        index_vals [1] :: INT AS array_index,
        OBJECT_AGG(
            key,
            value_
        ) AS receipts_json
    FROM
        {{ ref("bronze__overflowed_receipts") }}
    GROUP BY
        ALL
)
SELECT
    block_number,
    array_index,
    receipts_json,
    partition_key,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'array_index']
    ) }} AS receipts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bronze_overflowed_receipts qualify(ROW_NUMBER() over(PARTITION BY receipts_id
ORDER BY
    _inserted_timestamp DESC)) = 1
{% else %}
SELECT
    NULL :: INT AS block_number,
    NULL :: INT AS array_index,
    NULL :: OBJECT AS receipts_json,
    NULL :: INT AS partition_key,
    NULL :: timestamp_ltz AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'array_index']
    ) }} AS receipts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
{% endif %}