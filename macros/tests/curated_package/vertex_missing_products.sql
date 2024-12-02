{% test vertex_missing_products(
    model,
    filter) %}

with recent_records as (
    select 
        *
    from 
        {{ model }}
    where 
        {% if model == 'silver__vertex_market_stats' %}
            product_type = 'perp' AND product_id <> 0 AND BASE_VOLUME_24H > 0 AND product_id in (
                select product_id
                from (
                    select product_id,
                    min(hour) as min_hour 
                    from {{this}}
                    group by 1 
                    having min_hour <= SYSDATE() - INTERVAL '2 days'
                )
            )
            AND 
                modified_timestamp >= SYSDATE() - INTERVAL '7 days'
        {% else %}
            modified_timestamp >= SYSDATE() - INTERVAL '7 days'
        {% endif %}
),

invalid_product_ids as (
    select distinct product_id
    from {{ ref('silver__vertex_dim_products') }}
    where product_id not in (select product_id from recent_records)
    AND block_timestamp < sysdate() - INTERVAL '2 days'
    {% if filter %}
        AND {{ filter }}
    {% endif %}
)

select * 
from invalid_product_ids

{% endtest %}

{% test vertex_product_level_recency(
    model,
    filter) %}

with recent_records as (
    select distinct(product_id) from  {{model}}
    where block_timestamp >= SYSDATE() - INTERVAL '7 days'
),

invalid_product_ids as (
    select *
    from {{ ref('silver__vertex_dim_products') }}
    where product_id not in (select product_id from recent_records)
    AND block_timestamp < sysdate() - INTERVAL '2 days'
    {% if filter %}
        AND {{ filter }}
    {% endif %}
)

select * 
from invalid_product_ids

{% endtest %}