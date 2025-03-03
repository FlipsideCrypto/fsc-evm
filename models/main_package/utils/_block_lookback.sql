{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'ephemeral'
) }}

{% set uses_lookback = get_config_var('MAIN_SL_BLOCK_LOOKBACK_ENABLED', True) %}

{% if not uses_lookback %}

SELECT  
    0 AS block_number
{% else %}
SELECT
    COALESCE(MIN(block_number), 0) AS block_number
FROM
    {{ ref("core__fact_blocks") }}
WHERE
    block_timestamp >= DATEADD('hour', -72, TRUNCATE(SYSDATE(), 'HOUR'))
    AND block_timestamp < DATEADD('hour', -71, TRUNCATE(SYSDATE(), 'HOUR'))

{% endif %}