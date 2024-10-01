{% macro silver_labels() %}

{# Log configuration details if in execution mode #}
{%- if execute -%}
    {{ log("", info=True) }}
    {{ log("=== Model Configuration ===", info=True) }}
    {{ log("materialized: " ~ config.materialized, info=True) }}
    {{ log("unique_key: " ~ config.unique_key, info=True) }}
    {{ log("incremental_strategy: " ~ config.incremental_strategy, info=True) }}
    {{ log("merge_exclude_columns: " ~ config.merge_exclude_columns, info=True) }}
    {{ log("cluster_by: " ~ config.cluster_by, info=True) }}
    {{ log("post_hook: " ~ config.post_hook, info=True) }}
    {{ log("tags: " ~ config.tags, info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

{# Set up dbt configuration #}
{{ config(
    materialized = 'incremental',
    unique_key = ['address', 'blockchain'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = 'modified_timestamp::DATE',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, label_type, label_subtype, address_name, project_name), SUBSTRING(address, label_type, label_subtype, address_name, project_name); DELETE FROM {{ this }} WHERE _is_deleted = TRUE;",
    tags = ['non_realtime']
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
    labels_combined_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__labels') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
{% endmacro %}