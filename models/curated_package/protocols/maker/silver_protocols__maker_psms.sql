{{ config(
    materialized = 'table',
    unique_key = ['psm_address', 'ilk'],
    cluster_by = ['ilk'],
    tags = ['silver_protocols', 'maker', 'psm', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT DISTINCT
    u_address AS psm_address,
    ilk,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM {{ ref('maker__fact_vat_frob') }}
WHERE ilk LIKE 'PSM-%'
