{% macro streamline_decoded_complete(model) %}
SELECT
    block_number,
    file_name,
    id AS {% if model == 'decoded_logs' %}
        _log_id {% elif model == 'decoded_traces' %}
        _call_id
    {% endif %},
    {{ dbt_utils.generate_surrogate_key(['id']) }} AS complete_{{ model }}_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {% if is_incremental() %}
        {{ ref('bronze__streamline_' ~ model) }}
    WHERE
        _inserted_timestamp >= (
            SELECT
                COALESCE(MAX(_inserted_timestamp), '1970-01-01'::TIMESTAMP) AS _inserted_timestamp
            FROM
                {{ this }}
        )
    {% else %}
        {{ ref('bronze__streamline_fr_' ~ model) }}
    {% endif %}

QUALIFY (ROW_NUMBER() OVER (PARTITION BY id ORDER BY _inserted_timestamp DESC)) = 1

{% endmacro %}