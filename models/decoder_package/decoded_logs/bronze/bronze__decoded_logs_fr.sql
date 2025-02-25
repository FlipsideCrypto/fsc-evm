{# Log configuration details #}
{{ log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze_decoded_logs']
) }}

SELECT
    *
FROM
    {{ ref('bronze__decoded_logs_fr_v2') }}
{% if get_var('GLOBAL_SL_STREAMLINE_V1_ENABLED', false) %}
UNION ALL
SELECT
    *
FROM
    {{ ref('bronze__decoded_logs_fr_v1') }}
{% endif %}
