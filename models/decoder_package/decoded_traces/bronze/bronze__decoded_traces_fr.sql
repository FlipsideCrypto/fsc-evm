{{ config (
    materialized = 'view',
    tags = ['bronze_decoded_traces']
) }}

SELECT
    *
FROM
    {{ ref('bronze__decoded_traces_fr_v2') }}
{% if var('GLOBAL_USES_STREAMLINE_V1', false) %}
UNION ALL
SELECT
    *
FROM
    {{ ref('bronze__decoded_traces_fr_v1') }}
{% endif %}
