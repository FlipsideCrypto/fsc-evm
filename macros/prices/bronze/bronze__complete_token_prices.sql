{% macro bronze_complete_token_prices(
        token_addresses,
        blockchain = target.database | lower | replace(
            '_dev',
            ''
        )
    ) %}
SELECT
    HOUR,
    token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    price,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_imputed,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_prices_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_token_prices'
    ) }}
WHERE
    blockchain = '{{ blockchain }}' 
    {% if token_addresses %}
        AND token_address IN ({% if token_addresses is string %}
            '{{ token_addresses }}'
        {% else %}
            {{ token_addresses | replace('[', '') | replace(']', '') }}
        {% endif %})
    {% endif %}
{% endmacro %}
