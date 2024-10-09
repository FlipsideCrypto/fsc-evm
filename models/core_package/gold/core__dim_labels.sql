{% set post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, label_type, label_subtype, address_name, label), SUBSTRING(address, label_type, label_subtype, address_name, label); DELETE FROM {{ this }} WHERE address in (SELECT address FROM {{ ref('silver__labels') }} WHERE _is_deleted = TRUE);" %}

{%- if flags.WHICH == 'compile' and execute -%}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '"\n' %}
    {% set config_log = config_log ~ '    unique_key = ' ~ config.get('unique_key') ~ '\n' %}
    {% set config_log = config_log ~ '    incremental_strategy = "' ~ config.get('incremental_strategy') ~ '"\n' %}
    {% set config_log = config_log ~ '    merge_exclude_columns = ' ~ config.get('merge_exclude_columns') ~ '\n' %}
    {% set config_log = config_log ~ '    cluster_by = ' ~ config.get('cluster_by') ~ '\n' %}
    {% set config_log = config_log ~ '    post_hook = ' ~ post_hook ~ '\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}

{%- endif -%}

{{ config(
    materialized = 'incremental',
    unique_key = ['address', 'blockchain'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = 'modified_timestamp::DATE',
    post_hook = post_hook,
    tags = ['core']
) }}

SELECT
    blockchain,
    creator,
    address,
    address_name,
    label_type,
    label_subtype,
    project_name AS label,
    {{ dbt_utils.generate_surrogate_key(['labels_id']) }} AS dim_labels_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__labels') }} s 

{% if is_incremental() %}
WHERE
    s.modified_timestamp > (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}