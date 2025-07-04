{# Get variables #}
{% set vars = return_vars() %}

{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'view',
    tags = ['bronze','labels','phase_3']
) }}

SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    _is_deleted,
    modified_timestamp,
    labels_combined_id
FROM
    {{ source(
        'crosschain_silver',
        'labels_combined'
    ) }}
WHERE
    blockchain IN ({% if vars.MAIN_LABELS_BLOCKCHAINS is string %}
        '{{ vars.MAIN_LABELS_BLOCKCHAINS }}'
    {% else %}
        {{ vars.MAIN_LABELS_BLOCKCHAINS | replace('[', '') | replace(']', '') }}
    {% endif %})
    AND address LIKE '0x%'