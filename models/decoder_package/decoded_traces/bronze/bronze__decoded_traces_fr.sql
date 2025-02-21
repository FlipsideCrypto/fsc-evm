{# Log configuration details #}
{{ log_model_details() }}

{{ config (
    materialized = 'view',
    tags = ['bronze_decoded_traces']
) }}

SELECT
    *
FROM
    {{ ref('bronze__decoded_traces_fr_v2') }}
{% if var('GLOBAL_SL_STREAMLINE_V1_ENABLED', false) %}
UNION ALL
SELECT
    *
FROM
    {{ ref('bronze__decoded_traces_fr_v1') }}
{% endif %}
