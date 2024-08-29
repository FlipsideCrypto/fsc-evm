{% macro streamline_core_complete(
        model
    ) %}
SELECT
    COALESCE(
        VALUE :"BLOCK_NUMBER" :: INT,
        VALUE :"block_number" :: INT,
        metadata :request :"data" :id :: INT,
        PARSE_JSON(
            metadata :request :"data"
        ) :id :: INT
    ) AS block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS {% if model == 'blocks' %}
        complete_blocks_id {% elif model == 'transactions' %}
        complete_transactions_id {% elif model == 'receipts' %}
        complete_receipts_id {% elif model == 'traces' %}
        complete_traces_id {% elif model == 'confirmed_blocks' %}
        complete_confirmed_blocks_id
    {% endif %},
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{% if model == 'blocks' %}
    {{ ref('bronze__streamline_blocks') }}

    {% elif model == 'transactions' %}
    {{ ref('bronze__streamline_transactions') }}

    {% elif model == 'receipts' %}
    {{ ref('bronze__streamline_receipts') }}

    {% elif model == 'traces' %}
    {{ ref('bronze__streamline_traces') }}

    {% elif model == 'confirmed_blocks' %}
    {{ ref('bronze__streamline_confirmed_blocks') }}
{% endif %}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) AS _inserted_timestamp
        FROM
            {{ this }})
        {% else %}
            {% if model == 'blocks' %}
                {{ ref('bronze__streamline_fr_blocks') }}

                {% elif model == 'transactions' %}
                {{ ref('bronze__streamline_fr_transactions') }}

                {% elif model == 'receipts' %}
                {{ ref('bronze__streamline_fr_receipts') }}

                {% elif model == 'traces' %}
                {{ ref('bronze__streamline_fr_traces') }}

                {% elif model == 'confirmed_blocks' %}
                {{ ref('bronze__streamline_fr_confirmed_blocks') }}
            {% endif %}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY block_number
        ORDER BY
            _inserted_timestamp DESC)) = 1
{% endmacro %}
