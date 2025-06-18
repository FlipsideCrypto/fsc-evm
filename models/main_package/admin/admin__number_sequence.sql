{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    cluster_by = 'round(_id,-3)',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(_id)",
    full_refresh = false,
    tags = ['silver','admin','phase_1']
) }}

SELECT
    ROW_NUMBER() over (
        ORDER BY
            SEQ4()
    ) - 1 :: INT AS _id
FROM
    TABLE(GENERATOR(rowcount => {{ vars.GLOBAL_MAX_SEQUENCE_NUMBER }}))
WHERE 1=1
{% if is_incremental() %}
    AND 1=0
{% endif %}