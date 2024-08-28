{% macro streamline_core_complete(
        blocks = false,
        transactions = false,
        receipts = false,
        traces = false,
        confirmed_blocks = false
    ) %}
SELECT
    COALESCE(
        VALUE :"BLOCK_NUMBER" :: INT,
        VALUE :"block_number" :: INT
    ) AS block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS {% if blocks %}
        complete_blocks_id {% elif transactions %}
        complete_transactions_id {% elif receipts %}
        complete_receipts_id {% elif traces %}
        complete_traces_id {% elif confirmed_blocks %}
        complete_confirmed_blocks_id
    {% endif %},
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{% if blocks %}
    {{ ref('bronze__streamline_blocks') }}

    {% elif transactions %}
    {{ ref('bronze__streamline_transactions') }}

    {% elif receipts %}
    {{ ref('bronze__streamline_receipts') }}

    {% elif traces %}
    {{ ref('bronze__streamline_traces') }}

    {% elif confirmed_blocks %}
    {{ ref('bronze__streamline_confirmed_blocks') }}
{% endif %}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) AS _inserted_timestamp
        FROM
            {{ this }})
        {% else %}
            {% if blocks %}
                {{ ref('bronze__streamline_FR_blocks') }}

                {% elif transactions %}
                {{ ref('bronze__streamline_FR_transactions') }}

                {% elif receipts %}
                {{ ref('bronze__streamline_FR_receipts') }}

                {% elif traces %}
                {{ ref('bronze__streamline_FR_traces') }}

                {% elif confirmed_blocks %}
                {{ ref('bronze__streamline_FR_confirmed_blocks') }}
            {% endif %}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY block_number
        ORDER BY
            _inserted_timestamp DESC)) = 1
{% endmacro %}
