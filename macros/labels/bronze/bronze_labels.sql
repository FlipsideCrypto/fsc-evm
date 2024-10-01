{% macro bronze_labels() %}

{# Set macro parameters #}
{%- set blockchains = var('LABELS_BLOCKCHAINS', target.database | lower | replace('_dev','') ) -%}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("materialized: " ~ config.get('materialized'), info=True) }}
    {{ log("", info=True) }}
    {{ log("=== Parameters ===", info=True) }}
    {{ log("blockchains: " ~ blockchains, info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

{# Set up dbt configuration #}
{{ config(
    materialized = 'view'
) }}

{# Main query starts here #}
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
    blockchain IN ({% if blockchains is string %}
        '{{ blockchains }}'
    {% else %}
        {{ blockchains | replace('[', '') | replace(']', '') }}
    {% endif %})
    AND address LIKE '0x%'
{% endmacro %}
