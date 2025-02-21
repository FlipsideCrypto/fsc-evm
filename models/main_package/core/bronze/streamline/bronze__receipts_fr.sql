{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = 'view',
    tags = ['bronze_receipts']
) }}

SELECT
    partition_key,
    block_number,
    array_index,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
    {{ ref('bronze__receipts_fr_v2') }}
{% if var('GLOBAL_SL_STREAMLINE_V1_ENABLED', false) %}
UNION ALL
SELECT
    _partition_by_block_id AS partition_key,
    block_number,
    COALESCE(
          VALUE :"array_index" :: INT,
          TRY_TO_NUMBER(
              utils.udf_hex_to_int(
                  VALUE :"data" :"transactionIndex" :: STRING
              )
          )
      ) AS array_index,
    VALUE,
    DATA,
    metadata,
    file_name,
    _inserted_timestamp
FROM
   {{ ref('bronze__receipts_fr_v1') }}
{% endif %}