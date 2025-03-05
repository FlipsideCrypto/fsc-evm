import re

def extract_variables_from_return_vars(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Find all get_var calls
    pattern = r'{% set ns\.([A-Z_]+) = get_var\([\'"]([A-Z_]+)[\'"], (.*?)\) %}' 
    matches_get_var = re.findall(pattern, content)
    
    # Find all var calls
    var_pattern = r'{% set ns\.([A-Z_]+) = var\([\'"]([A-Z_]+)[\'"], (.*?)\) %}'
    matches_var = re.findall(var_pattern, content)
    
    # Combine matches
    matches = matches_get_var + matches_var
    
    variables = {}
    for var_name, key, default in matches:
        # Parse the package and category from the key
        parts = key.split('_')
        if len(parts) >= 3:
            package = parts[0]
            category = parts[1]
        else:
            package = parts[0]
            category = "NULL"
        
        # Determine data type from default value
        if default.lower() in ('true', 'false'):
            data_type = 'BOOLEAN'
        elif default.isdigit() or (default.startswith('-') and default[1:].isdigit()):
            data_type = 'NUMBER'
        elif default.startswith('{') or default.startswith('['):
            data_type = 'OBJECT'
        elif default == 'none':
            data_type = 'NULL'
        else:
            data_type = 'STRING'
        
        variables[key] = {
            'package': package,
            'category': category,
            'data_type': data_type,
            'default': default,
            'var_name': var_name
        }
    
    return variables

def generate_master_keys_macro(variables):
    # Reorganize variables by package and category
    organized_vars = {}
    for key, info in variables.items():
        package = info['package']
        category = info['category']
        
        if package not in organized_vars:
            organized_vars[package] = {}
        
        if category not in organized_vars[package]:
            organized_vars[package][category] = {}
            
        organized_vars[package][category][key] = {
            'data_type': info['data_type'],
            'default': info['default'],
            'var_name': info['var_name']
        }
    
    # Generate the macro content with the new hierarchy and improved formatting
    macro_content = """{% macro master_vars_keys() %}
    {% set master_keys = {
"""
    
    for package, categories in organized_vars.items():
        macro_content += f"        '{package}': {{\n"
        
        for category, keys in categories.items():
            macro_content += f"            '{category}': {{\n"
            
            for key, info in keys.items():
                # Format with line breaks for better readability
                macro_content += f"                '{key}': {{\n"
                macro_content += f"                    'data_type': '{info['data_type']}',\n"
                
                # Always wrap default values in quotes to treat them as strings
                if info['data_type'] == 'OBJECT':
                    macro_content += f"                    'default': '{{}}'\n"
                else:
                    # Escape any single quotes in the default value
                    default_value = str(info['default']).replace("'", "\\'")
                    macro_content += f"                    'default': '{default_value}'\n"
                
                macro_content += "                },\n"
            
            macro_content += "            },\n"
        
        macro_content += "        },\n"
    
    macro_content += """    } %}
    
    {{ return(master_keys) }}
{% endmacro %}"""
    
    return macro_content

# Extract variables and generate macro
variables = extract_variables_from_return_vars('macros/utilities/variables/return_vars.sql')
macro_content = generate_master_keys_macro(variables)

# Write to file
with open('macros/utilities/variables/project_vars/_master_keys.sql', 'w') as f:
    f.write(macro_content)
