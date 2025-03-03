{% macro convert_value(value, data_type) %}
    {% if value is none %}
        {{ return(none) }}
    {% endif %}
    
    {% set data_type = data_type | lower %}
    
    {% if data_type in ['number', 'integer', 'fixed', 'float', 'decimal'] %}
        {% if '.' in value | string %}
            {{ return(value | float) }}
        {% else %}
            {{ return(value | int) }}
        {% endif %}
    {% elif data_type == 'boolean' %}
        {{ return(value | string | lower == 'true') }}
    {% elif data_type in ['json', 'variant', 'object'] %}
        {{ return(fromjson(value)) }}
    {% elif data_type == 'array' %}
        {% set array_values = value.split(',') %}
        {% set converted_array = [] %}
        {% for val in array_values %}
            {% set stripped_val = val.strip() %}
            {% if stripped_val.isdigit() %}
                {% do converted_array.append(stripped_val | int) %}
            {% elif stripped_val.replace('.','',1).isdigit() %}
                {% do converted_array.append(stripped_val | float) %}
            {% elif stripped_val.lower() in ['true', 'false'] %}
                {% do converted_array.append(stripped_val.lower() == 'true') %}
            {% else %}
                {% do converted_array.append(stripped_val) %}
            {% endif %}
        {% endfor %}
        {{ return(converted_array) }}
    {% else %}
        {{ return(value) }}
    {% endif %}
{% endmacro %}

{% macro evaluate_expression(expression, variables_dict) %}
    {% if expression is string and '{{' in expression and '}}' in expression %}
        {% set template = expression %}
        {% set regex = r'\{\{([^}]+)\}\}' %}
        
        {% set matches = modules.re.findall(regex, template) %}
        {% for match in matches %}
            {% set var_expr = match.strip() %}
            {% set result = none %}
            
            {# Try to evaluate the expression using the variables dictionary #}
            {% set expr_parts = var_expr.split(' ') %}
            {% if expr_parts | length == 1 %}
                {# Simple variable reference #}
                {% set result = variables_dict.get(var_expr) %}
            {% elif expr_parts | length == 3 %}
                {# Simple math operation: var1 op var2 #}
                {% set var1 = variables_dict.get(expr_parts[0]) %}
                {% set op = expr_parts[1] %}
                {% set var2 = variables_dict.get(expr_parts[2]) %}
                
                {# If var2 is not in the dict, try to convert it to a number #}
                {% if var2 is none %}
                    {% if expr_parts[2].isdigit() %}
                        {% set var2 = expr_parts[2] | int %}
                    {% elif expr_parts[2].replace('.','',1).isdigit() %}
                        {% set var2 = expr_parts[2] | float %}
                    {% endif %}
                {% endif %}
                
                {% if var1 is not none and var2 is not none %}
                    {% if op == '*' %}
                        {% set result = var1 * var2 %}
                    {% elif op == '+' %}
                        {% set result = var1 + var2 %}
                    {% elif op == '-' %}
                        {% set result = var1 - var2 %}
                    {% elif op == '/' %}
                        {% set result = var1 / var2 %}
                    {% endif %}
                {% endif %}
            {% endif %}
            
            {% if result is not none %}
                {% set template = template | replace('{{' ~ var_expr ~ '}}', result | string) %}
            {% endif %}
        {% endfor %}
        
        {# Try to convert the final result to a number if possible #}
        {% if template.isdigit() %}
            {{ return(template | int) }}
        {% elif template.replace('.','',1).isdigit() %}
            {{ return(template | float) }}
        {% else %}
            {{ return(template) }}
        {% endif %}
    {% else %}
        {{ return(expression) }}
    {% endif %}
{% endmacro %} 