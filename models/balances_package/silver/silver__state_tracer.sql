{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

-- depends_on: {{ ref('bronze__state_tracer') }}

{{ config (
    materialized = "incremental",
    unique_key = ['block_number'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['modified_timestamp::date', 'partition_key'],
    full_refresh = vars.GLOBAL_SILVER_FR_ENABLED,
    tags = ['silver','balances','phase_4']
) }}

SELECT
    partition_key,
    block_number,
    array_index AS tx_position,
    DATA :txHash :: STRING AS tx_hash,
    DATA :result :pre :: variant AS pre_state_json,
    DATA :result :post :: variant AS post_state_json,
    DATA AS state_json,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'tx_position']) }} AS state_tracer_id,
    _inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__state_tracer') }}
WHERE
    _inserted_timestamp > (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) AS _inserted_timestamp
        FROM
            {{ this }})
            AND DATA IS NOT NULL
        {% else %}
            {{ ref('bronze__state_tracer_fr') }}
        WHERE
            DATA IS NOT NULL
        {% endif %}
--temp filters for testing
AND partition_key IN (ROUND(24817293,-3),ROUND(24817294,-3))
AND block_number IN (24817293,24817294)

        qualify (ROW_NUMBER() over (PARTITION BY block_number, tx_position
        ORDER BY
            _inserted_timestamp DESC)) = 1