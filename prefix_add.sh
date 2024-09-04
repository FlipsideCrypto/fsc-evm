#!/bin/bash

# Define the root directory containing .md files
ROOT_DIR="doc_descriptions"

# Function to add evm_ prefix to each doc block in the file with enhanced debugging
add_evm_prefix() {
    local file="$1"
    local modified=0

    echo "Reading file: $file"
    
    # Read each line and process it
    while IFS= read -r line || [[ -n "$line" ]]; do
        echo "Original line: '$line'" # Print each line being processed

        # Check if the line contains '{% docs', more liberally
        if [[ "$line" == *"{% docs"* ]]; then
            echo "Detected potential doc block line: '$line'"

            # Check if the line already has evm_ prefix
            if [[ "$line" =~ \{%\s*docs\s+evm_ ]]; then
                echo "Doc block already has evm_ prefix, no changes needed."
            else
                echo "Adding evm_ prefix to doc block."

                # Extract the part before and after the doc name
                prefix="${line%%docs *}docs "
                doc_name="${line#*docs }"
                doc_name="${doc_name%\%*}" # Remove everything after the doc name
                suffix="%}"

                # Form the updated line with the evm_ prefix
                updated_line="${prefix}evm_${doc_name}${suffix}"
                echo "Updated line: '$updated_line'"
                line="$updated_line"
                modified=1
            fi
        else
            echo "No doc block detected in line."
        fi
        
        # Write the (possibly modified) line to a temporary file
        echo "$line" >> "$file.tmp"
    done < "$file"

    # Replace the original file with the updated version only if modifications were made
    if [[ $modified -eq 1 ]]; then
        echo "Changes made, updating file..."
        mv "$file.tmp" "$file"
    else
        echo "No changes needed, cleaning up..."
        rm -f "$file.tmp"
    fi
}

# Iterate over all .md files in the directory and its subdirectories
find "$ROOT_DIR" -type f -name "*.md" | while read -r file; do
    echo "Processing $file..."
    add_evm_prefix "$file"
done

echo "Prefix addition process completed."