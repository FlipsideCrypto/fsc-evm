{% macro block_reorg(reorg_model_list, hours) %}
  {% if reorg_model_list %}
    {% set models = reorg_model_list.split(",") %}
  {% else %}
    {# Default: get downstream models of core__fact_event_logs, excluding the event logs model itself #}
    {% set target_model_name = 'core__fact_event_logs' %}
    {% set target_unique_id = 'model.fsc_evm.core__fact_event_logs' %}
    
    {# Get all downstream models of core__fact_event_logs #}
    {% set model_nodes = [] %}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model' and node.config.materialized != 'ephemeral' %}
        {# Check if this node depends on core__fact_event_logs #}
        {% if target_unique_id in node.depends_on.nodes %}
          {% set model_nodes = model_nodes.append(node.name) %}
        {% endif %}
      {% endif %}
    {% endfor %}
    {% set models = model_nodes %}
    {{ log("Found " ~ models | length ~ " downstream models of " ~ target_model_name, info=true) }}
  {% endif %}
  
  {% for model in models %}
    {% set relation = ref(model) %}
    
    {# Check if this is a table (not a view) #}
    {% set relation_type = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) %}
    {% if relation_type and relation_type.type != 'table' %}
      {{ log("⚠️  Skipping " ~ model ~ " - not a table (type: " ~ relation_type.type ~ ")", info=true) }}
      {% continue %}
    {% endif %}
    
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% set column_names = columns | map(attribute='name') | map('lower') | list %}
    
    {% if '_inserted_timestamp' in column_names %}
      {% set timestamp_col = '_inserted_timestamp' %}
    {% elif 'modified_timestamp' in column_names %}
      {% set timestamp_col = 'modified_timestamp' %}
    {% else %}
      {{ log("⚠️  Skipping " ~ model ~ " - no timestamp column found", info=true) }}
      {% continue %}
    {% endif %}
    
    {# Check if required columns exist for reorg logic #}
    {% if 'block_number' not in column_names or 'tx_hash' not in column_names %}
      {{ log("⚠️  Skipping " ~ model ~ " - missing block_number or tx_hash columns", info=true) }}
      {% continue %}
    {% endif %}
    
    {# First, count how many rows will be deleted #}
    {% set count_sql %}
      SELECT COUNT(*) as row_count
      FROM {{ relation }} t
      left join {{ ref('silver__confirm_blocks') }} cb using (block_number, tx_hash)
      where t.block_timestamp between dateadd('day',-10,sysdate()) and dateadd('hour',-12,sysdate()) 
      and cb.tx_hash is null;
    {% endset %}
    
    {% set count_result = run_query(count_sql) %}
    {% set rows_to_delete = count_result.columns[0].values()[0] %}
    
    {% if rows_to_delete > 0 %}
      {% set sql %}
        DELETE FROM {{ relation }} t
        WHERE t.block_timestamp between dateadd('day',-10,sysdate()) and dateadd('hour',-12,sysdate())
        AND NOT EXISTS (
          SELECT 1 FROM {{ ref('silver__confirm_blocks') }} cb 
          WHERE cb.block_number = t.block_number 
          AND cb.tx_hash = t.tx_hash
        );
      {% endset %}
      {% do run_query(sql) %}
      {{ log("✅ DELETED " ~ rows_to_delete ~ " rows from " ~ model, info=true) }}
    {% else %}
      {{ log("ℹ️  No rows to delete from " ~ model, info=true) }}
    {% endif %}
  {% endfor %}
{% endmacro %}