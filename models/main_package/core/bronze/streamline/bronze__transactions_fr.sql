{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = 'view',
    tags = ['bronze_core']
) }}

SELECT
    partition_key,
    block_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__transactions_fr_v2') }}
{% if var('GLOBAL_SL_STREAMLINE_V1_ENABLED', false) %}
UNION ALL
SELECT
    _partition_by_block_id AS partition_key,
    block_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
   {{ ref('bronze__transactions_fr_v1') }}
{% endif %}
{% if var('MAIN_SL_BLOCKS_TRANSACTIONS_PATH_ENABLED', false) %}
UNION ALL
SELECT
    partition_key,
    block_number,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__transactions_fr_v2_1') }}
{% endif %}