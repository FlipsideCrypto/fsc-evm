{% macro balances_erc20_history() %}

  {% set vars = return_vars() %}

  {%- set params = {
      "external_table": 'balances_erc20',
      "sql_limit": vars.BALANCES_SL_ERC20_DAILY_HISTORY_SQL_LIMIT,
      "producer_batch_size": vars.BALANCES_SL_ERC20_DAILY_HISTORY_PRODUCER_BATCH_SIZE,
      "worker_batch_size": vars.BALANCES_SL_ERC20_DAILY_HISTORY_WORKER_BATCH_SIZE,
      "async_concurrent_requests": vars.BALANCES_SL_ERC20_DAILY_HISTORY_ASYNC_CONCURRENT_REQUESTS
  } -%}

  {% set find_batches_query %}
    SELECT 
      DISTINCT batch_id
    FROM {{ ref('streamline__balances_erc20_daily_history_to_do') }}
    ORDER BY batch_id ASC
  {% endset %}

  {% set results = run_query(find_batches_query) %}

  {% if execute %}
    {% set batches = results.columns[0].values() %}
    
    {% for batch in batches %}
      {% set view_name = 'balances_erc20_daily_history_' ~ batch %}
      
      {% set create_view_query %}
        create or replace view streamline.{{view_name}} as (
            SELECT
                block_number,
                block_date_unix,
                address,
                contract_address,
                partition_key,
                live.udf_api(
                    'POST',
                    '{{ vars.GLOBAL_NODE_URL }}',
                    OBJECT_CONSTRUCT(
                        'Content-Type',
                        'application/json',
                        'fsc-quantum-state',
                        'streamline'
                    ),
                    api_request,
                    '{{ vars.GLOBAL_NODE_VAULT_PATH }}'
                ) AS request
            FROM
                {{ ref('streamline__balances_erc20_daily_history_to_do') }}
                WHERE batch_id = {{ batch }}
            ORDER BY partition_key DESC, block_number DESC
            LIMIT {{ params.sql_limit }}
        )
      {% endset %}

      {# Create the view #}
      {% do run_query(create_view_query) %}
      {{ log("Created view for batch " ~ batch, info=True) }}

      {% if var("STREAMLINE_INVOKE_STREAMS", false) %}
        {# Use fsc_utils.if_data_call_function_v2 like the model approach #}
        {% set batch_params = params.copy() %}
        {% do batch_params.update({"sql_source": view_name}) %}
        
        {% set function_call_sql %}
        {{ fsc_utils.if_data_call_function_v2(
            func = 'streamline.udf_bulk_rest_api_v2',
            target = this.schema ~ "." ~ view_name,
            params = batch_params
        ) }}
        {% endset %}

        {% do run_query(function_call_sql) %}
        {{ log("Streamline function call for batch " ~ batch ~ ": " ~ function_call_sql, info=true) }}
      {% endif %}
      
    {% endfor %}
  {% endif %}

{% endmacro %}