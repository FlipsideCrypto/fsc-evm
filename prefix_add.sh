#!/bin/bash

# Define the root directory containing .md files
ROOT_DIR="doc_descriptions"

# Function to add the evm_ prefix if missing and print changes
add_evm_prefix() {
    local file="$1"
    local modified=0
    
    # Use awk to read and modify lines as needed
    awk '
    {
        # Check for the start of a doc block
        if ($0 ~ /{%\s*docs\s+([a-zA-Z_]+)\s*%}/) {
            # Capture the original doc name
            original = $0;
            sub("{% docs ", "", original);
            sub(" %}", "", original);
            
            # Check if the doc name starts with evm_
            if (original !~ /^evm_/) {
                # Print the update being made
                print "Updating in " FILENAME ": " $0 " -> {% docs evm_" original " %}";
                
                # Update the line to add evm_ prefix
                gsub("{% docs " original " %}", "{% docs evm_" original " %}");
                modified = 1;
            }
        }
        # Print the current line (modified or not)
        print;
    }' "$file" > "$file.tmp" && mv "$file.tmp" "$file"

    # Check if any modifications were made
    if [[ $modified -eq 0 ]]; then
        echo "No changes needed in $file"
    fi
}

# Find all .md files in the directory and its subdirectories
find "$ROOT_DIR" -type f -name "*.md" | while read -r file; do
    echo "Processing $file..."
    add_evm_prefix "$file"
done

echo "Prefix addition process completed."