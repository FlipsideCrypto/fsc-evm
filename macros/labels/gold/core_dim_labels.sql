{% macro core_dim_labels() %}
SELECT
    blockchain,
    creator,
    address,
    address_name,
    label_type,
    label_subtype,
    project_name AS label,
    labels_combined_id AS dim_labels_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__labels') }}
{% endmacro %}
