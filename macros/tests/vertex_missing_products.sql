{% test vertex_missing_products(
    model,
    filter) %}

with recent_records as (
    select * from  {{model}}
    where modified_timestamp >= SYSDATE() - INTERVAL '7 days'
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