{# Get Variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

SELECT DISTINCT ilk
FROM (
    SELECT ilk
    FROM {{ ref('maker__fact_vat_frob') }}

    UNION

    SELECT ilk
    FROM {{ ref('fact_spot_file') }}

    UNION

    SELECT ilk
    FROM {{ ref('fact_jug_file') }}
)