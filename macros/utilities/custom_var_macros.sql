{% macro initialize_vars() %}
  {# This macro loads all variables and computes derived values just once per dbt run #}
  {# It's silently invoked by get_var - users don't need to call this directly #}
  
  {% if not this.vars_initialized or this.chain != var('chain', none) %}
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
          {% do default_vars.update({row['key']: row['default_value']}) %}
          {% do var_types.update({row['key']: row['data_type']}) %}
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
          {# Handle parent-child relationships #}
          {% if row['parent_key'] %}
            {# For hierarchical variables (like token mappings) #}
            {% if row['parent_key'] not in chain_vars %}
              {% do chain_vars.update({row['parent_key']: {}}) %}
            {% endif %}
            
            {# Ensure we have a dictionary for this parent #}
            {% if chain_vars[row['parent_key']] is not mapping %}
              {% do chain_vars.update({row['parent_key']: {}}) %}
            {% endif %}
            
            {# Add the child to the parent dictionary #}
            {% do chain_vars[row['parent_key']].update({row['key']: row['value']}) %}
          {% else %}
            {# Regular variables #}
            {% do chain_vars.update({row['key']: row['value']}) %}
          {% endif %}
        {% endfor %}
      {% endif %}
    {% endif %}
    
    {# STEP 3: Merge defaults and chain overrides #}
    {% set all_vars = {} %}
    
    {# Start with basic default values (not template expressions) #}
    {% for key, value in default_vars.items() %}
      {% if value is string and '{{' in value and '}}' in value %}
        {# Skip template expressions for now - we'll handle these after basic vars are set #}
        {# This prevents dependency ordering issues #}
      {% else %}
        {% do all_vars.update({key: value}) %}
      {% endif %}
    {% endfor %}
    
    {# Apply chain overrides to basic values #}
    {% for key, value in chain_vars.items() %}
      {# Skip parent-child relationships for now #}
      {% if value is not mapping %}
        {% do all_vars.update({key: value}) %}
      {% endif %}
    {% endfor %}
    
    {# STEP 4: Process template expressions in defaults #}
    {# This requires multiple passes to handle dependencies #}
    {% set template_vars = {} %}
    {% for key, value in default_vars.items() %}
      {% if value is string and '{{' in value and '}}' in value %}
        {% do template_vars.update({key: value}) %}
      {% endif %}
    {% endfor %}
    
    {# Process templates in multiple passes to handle dependencies #}
    {% for _ in range(3) %}  {# 3 passes should handle most dependency chains #}
      {% for key, template in template_vars.items() %}
        {% if key not in all_vars or all_vars[key] is none %}
          {# Create a context dictionary instead of using namespace #}
          {% set temp_context = {} %}
          {% for var_key, var_value in all_vars.items() %}
            {% do temp_context.update({var_key: var_value}) %}
          {% endfor %}
          
          {% set rendered_value = none %}
          {% try %}
            {# Evaluate the template in the context of current vars #}
            {% set rendered_value = render(template, temp_context) %}
            {% do all_vars.update({key: rendered_value}) %}
          {% except %}
            {# If it fails, probably due to dependencies, we'll try again in next pass #}
          {% endtry %}
        {% endif %}
      {% endfor %}
    {% endfor %}
    
    {# STEP 5: Apply hierarchical chain overrides #}
    {% for parent_key, children in chain_vars.items() %}
      {% if children is mapping %}
        {# Handle the hierarchical variables (like token mappings) #}
        {% do all_vars.update({parent_key: children}) %}
      {% endif %}
    {% endfor %}
    
    {# STEP 6: Convert all values to appropriate data types based on var_types #}
    {% set final_vars = {} %}
    {% for key, value in all_vars.items() %}
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
          {# Keep dictionaries as they are (for hierarchical variables) #}
          {% do final_vars.update({key: value}) %}
        {% else %}
          {% do final_vars.update({key: value}) %}
        {% endif %}
      {% else %}
        {# No type info, keep as is #}
        {% do final_vars.update({key: value}) %}
      {% endif %}
    {% endfor %}
    
    {# Store the variables and chain in the context for future use #}
    {% do this.update({
      'vars_initialized': true,
      'chain': current_chain,
      'vars': final_vars
    }) %}
  {% endif %}
{% endmacro %}

{% macro get_var(var_name, default_value=none) %}
  {# The main function that models will use to access variables #}
  {# First ensure variables are loaded - this only runs expensive operations once per dbt run #}
  {{ initialize_vars() }}
  
  {# Check dbt native vars first - project-level overrides take precedence #}
  {% if var(var_name, none) is not none and var_name != 'chain' %}
    {{ return(var(var_name)) }}
  {% endif %}
  
  {# Handle hierarchical variable access with dot notation, e.g., get_var('CURATED_VERTEX_TOKEN_MAPPING.USDC') #}
  {% if '.' in var_name %}
    {% set parts = var_name.split('.') %}
    {% set parent = parts[0] %}
    {% set child = parts[1] %}
    
    {% if parent in this.vars and this.vars[parent] is mapping and child in this.vars[parent] %}
      {{ return(this.vars[parent][child]) }}
    {% else %}
      {{ return(default_value) }}
    {% endif %}
  {% endif %}
  
  {# Check our variable dictionary for simple variable #}
  {% if var_name in this.vars %}
    {{ return(this.vars[var_name]) }}
  {% else %}
    {# Finally, fall back to default #}
    {{ return(default_value) }}
  {% endif %}
{% endmacro %}