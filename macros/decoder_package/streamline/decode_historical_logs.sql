{% macro decode_historical_logs() %}

  {%- set params = {
      "sql_limit": var("HISTORICAL_DECODING_SQL_LIMIT", 2000000),
      "producer_batch_size": var("HISTORICAL_DECODING_PRODUCER_BATCH_SIZE", 400000),
      "worker_batch_size": var("HISTORICAL_DECODING_WORKER_BATCH_SIZE", 200000)
  } -%}

  {% set find_weeks_query %}
    select distinct date_trunc('week', block_timestamp)::date as week
    from {{ ref('core__fact_blocks') }}
  {% endset %}

  {% set results = run_query(find_weeks_query) %}

  {% if execute %}
    {% set weeks = results.columns[0].values() %}
    
    {% for week in weeks %}
      {% set view_name = 'decode_historical_event_logs_' ~ week.strftime('%Y_%m_%d') %}
      
      {% set create_view_query %}
        create or replace view streamline.{{view_name}} as (
          SELECT
              l.block_number,
              concat(l.tx_hash::string, '-', l.event_index::string) as _log_id,
              A.abi AS abi,
              OBJECT_CONSTRUCT(
                  'topics',
                  l.topics,
                  'data',
                  l.data,
                  'address',
                  l.contract_address
              ) AS DATA
          FROM
              {{ ref('core__fact_event_logs') }} l
              INNER JOIN {{ ref('silver__complete_event_abis') }} A
              ON A.parent_contract_address = l.contract_address
              AND A.event_signature = l.topics[0]::STRING
              AND l.block_number BETWEEN A.start_block AND A.end_block
              LEFT JOIN (
                select _log_id 
                from {{ ref('streamline__decoded_logs_complete') }}
                join (
                    select block_number
                    from {{ ref('core__fact_blocks') }}
                    where date_trunc('week', block_timestamp) = '{{week}}'::timestamp
                ) 
                using (block_number)              
              ) dlc
              ON dlc._log_id = concat(l.tx_hash::string, '-', l.event_index::string)
          WHERE
              l.tx_succeeded
              AND date_trunc('week', block_timestamp) = '{{week}}'::timestamp
              AND dlc._log_id is null
              AND l.block_number < (
                SELECT
                    block_number
                FROM
                    {{ ref('_block_lookback') }}
              )
          LIMIT {{ params.sql_limit }}
        )
      {% endset %}

      {# Create the view #}
      {% do run_query(create_view_query) %}
      {{ log("Created view for week starting " ~ week.strftime('%Y-%m-%d'), info=True) }}
      
      {% if var("UPDATE_UDFS_AND_SPS", false) %}
        {# Invoke streamline, if rows exist to decode #}
        {% set decode_query %}
          SELECT
            streamline.udf_bulk_decode_logs_v2(
              PARSE_JSON(
                  $${ "external_table": "decoded_logs",
                  "producer_batch_size": {{ params.producer_batch_size }},
                  "sql_limit": {{ params.sql_limit }},
                  "sql_source": "{{view_name}}",
                  "worker_batch_size": {{ params.worker_batch_size }} }$$
              )
            )
          WHERE
            EXISTS(
              SELECT 1
              FROM streamline.{{view_name}}
              LIMIT 1
            );
        {% endset %}
        
        {% do run_query(decode_query) %}
        {{ log("Triggered decoding for " ~ week.strftime('%Y-%m-%d'), info=True) }}
        
        {# Call wait to avoid queueing up too many jobs #}
        {% do run_query("call system$wait(20)") %}
        {{ log("Completed wait after decoding for " ~ week.strftime('%Y-%m-%d'), info=True) }}
      {% endif %}
      
    {% endfor %}
  {% endif %}

{% endmacro %}