{{ config(
    materialized = 'table',
    unique_key = ['ilk'],
    cluster_by = ['ilk'],
    tags = ['silver_protocols', 'maker', 'ilk_list', 'curated']
) }}

{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT DISTINCT
    ilk,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM (
    SELECT ilk
    FROM {{ ref('maker__fact_vat_frob') }}

    UNION

    SELECT ilk
    FROM {{ ref('silver_protocols__maker_spot_file') }}

    UNION

    SELECT ilk
    FROM {{ ref('silver_protocols__maker_jug_file') }}
)
