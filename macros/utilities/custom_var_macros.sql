{% macro initialize_vars() %}
  {# This macro loads all variables and computes derived values just once per dbt run #}
  {# It's silently invoked by get_var - users don't need to call this directly #}
  
  {% if not context.get('vars_initialized', false) or context.get('current_chain', none) != var('chain', none) %}
    {% set current_chain = var('chain', none) %}
    
    {# STEP 1: Load default variable definitions with metadata #}
    {% set default_vars = {} %}
    {% set var_types = {} %}
    {% if execute %}
      {% set defaults_query %}
        select 
          key, 
          data_type,
          default_value
        from {{ ref('bronze__master_variable_keys') }}
      {% endset %}
      
      {% set default_results = run_query(defaults_query) %}
      {% if default_results and default_results.columns %}
        {% for row in default_results %}
          {% do var_types.update({row['key']: row['data_type']}) %}
          {% set value = row['default_value'] %}
          {% if value is not none %}
            {% do default_vars.update({row['key']: value}) %}
          {% endif %}
        {% endfor %}
      {% endif %}
    {% endif %}
    
    {# STEP 2: Load chain-specific variable overrides #}
    {% set chain_vars = {} %}
    {% if execute and current_chain %}
      {% set chain_query %}
        select 
          key,
          parent_key,
          value
        from {{ ref('bronze__master_variable_values') }}
        where chain = '{{ current_chain }}'
        and is_enabled = true
      {% endset %}
      
      {% set chain_results = run_query(chain_query) %}
      {% if chain_results and chain_results.columns %}
        {% for row in chain_results %}
          {% set value = row['value'] %}
          {% if value is not none %}
            {% if row['parent_key'] %}
              {% if row['parent_key'] not in chain_vars %}
                {% do chain_vars.update({row['parent_key']: {}}) %}
              {% endif %}
              {% if chain_vars[row['parent_key']] is mapping %}
                {% do chain_vars[row['parent_key']].update({row['key']: value}) %}
              {% endif %}
            {% else %}
              {% do chain_vars.update({row['key']: value}) %}
            {% endif %}
          {% endif %}
        {% endfor %}
      {% endif %}
    {% endif %}
    
    {# STEP 3: Merge defaults and chain overrides safely #}
    {% set all_vars = {} %}
    {% for key, value in default_vars.items() %}
      {% if value is string and '{{' in value and '}}' in value %}
        {# Skip template expressions for now #}
      {% else %}
        {% do all_vars.update({key: value}) %}
      {% endif %}
    {% endfor %}
    
    {% for key, value in chain_vars.items() %}
      {% if value is not mapping %}
        {% do all_vars.update({key: value}) %}
      {% endif %}
    {% endfor %}
    
    {# STEP 4: Process template expressions #}
    {% set template_vars = {} %}
    {% for key, value in default_vars.items() %}
      {% if value is string and '{{' in value and '}}' in value %}
        {% do template_vars.update({key: value}) %}
      {% endif %}
    {% endfor %}
    
    {% for _ in range(3) %}
      {% for key, template in template_vars.items() %}
        {% if key not in all_vars or all_vars[key] is none %}
          {% set context_dict = {} %}
          {% for var_key, var_value in all_vars.items() %}
            {% if var_value is not none %}
              {% if var_value is mapping %}
                {% do context_dict.update({var_key: var_value}) %}
              {% else %}
                {% do context_dict.update({var_key: var_value|string}) %}
              {% endif %}
            {% endif %}
          {% endfor %}
          
          {% if execute %}
            {% set template_str = template %}
            {% if template is not string %}
              {% set template_str = template|string %}
            {% endif %}
            
            {% set rendered_value = modules.jinja2.Template(template_str).render(**context_dict) %}
            {% if rendered_value is not none %}
              {% do all_vars.update({key: rendered_value}) %}
            {% endif %}
          {% endif %}
        {% endif %}
      {% endfor %}
    {% endfor %}
    
    {# STEP 5: Apply hierarchical chain overrides #}
    {% for parent_key, children in chain_vars.items() %}
      {% if children is mapping %}
        {% do all_vars.update({parent_key: children}) %}
      {% endif %}
    {% endfor %}
    
    {# STEP 6: Convert values to appropriate types #}
    {% set final_vars = {} %}
    {% for key, value in all_vars.items() %}
      {% if value is not none %}
        {% if key in var_types %}
          {% set data_type = var_types[key]|upper %}
          {% if value is string %}
            {% if data_type == 'BOOLEAN' %}
              {% if value|lower == 'true' %}
                {% do final_vars.update({key: true}) %}
              {% elif value|lower == 'false' %}
                {% do final_vars.update({key: false}) %}
              {% else %}
                {% do final_vars.update({key: value}) %}
              {% endif %}
            {% elif data_type == 'NUMBER' %}
              {% if value is match('^-?\d+(\.\d+)?$') %}
                {% do final_vars.update({key: value|float|int}) %}
              {% else %}
                {% do final_vars.update({key: value}) %}
              {% endif %}
            {% else %}
              {% do final_vars.update({key: value}) %}
            {% endif %}
          {% elif value is mapping %}
            {% do final_vars.update({key: value}) %}
          {% else %}
            {% do final_vars.update({key: value|string}) %}
          {% endif %}
        {% else %}
          {% do final_vars.update({key: value}) %}
        {% endif %}
      {% endif %}
    {% endfor %}
    
    {# Store the variables and chain in the context #}
    {% set _ = context.update({'vars_initialized': true, 'current_chain': current_chain, 'custom_vars': final_vars}) %}
  {% endif %}
{% endmacro %}

{% macro get_var(var_name, default_value=none) %}
  {{ initialize_vars() }}
  
  {# Check dbt native vars first #}
  {% if var(var_name, none) is not none and var_name != 'chain' %}
    {{ return(var(var_name)) }}
  {% endif %}
  
  {# Handle hierarchical variable access #}
  {% if '.' in var_name %}
    {% set parts = var_name.split('.') %}
    {% set parent = parts[0] %}
    {% set child = parts[1] %}
    
    {% if parent in context.custom_vars and context.custom_vars[parent] is mapping and child in context.custom_vars[parent] %}
      {{ return(context.custom_vars[parent][child]) }}
    {% else %}
      {{ return(default_value) }}
    {% endif %}
  {% endif %}
  
  {# Check our variable dictionary #}
  {% if var_name in context.custom_vars %}
    {{ return(context.custom_vars[var_name]) }}
  {% else %}
    {{ return(default_value) }}
  {% endif %}
{% endmacro %}