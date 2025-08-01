{# Log configuration details #}
{{ log_model_details() }}

{{ config(
    materialized = 'incremental',
    unique_key = ['address', 'blockchain'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = 'modified_timestamp::DATE',
    post_hook = "DELETE FROM {{ this }} WHERE address in (SELECT address FROM {{ ref('silver__labels') }} WHERE _is_deleted = TRUE);", -- Search optimization moved to daily_search_optimization maintenance model
    tags = ['gold','labels','phase_3']
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
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }}
    )
{% endif %}