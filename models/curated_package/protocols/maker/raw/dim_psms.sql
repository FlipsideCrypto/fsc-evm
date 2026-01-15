{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT DISTINCT
    u_address as psm_address,
    ilk
FROM {{ ref('maker__fact_vat_frob') }}
WHERE ilk LIKE 'PSM-%'