#!/bin/bash

# Define the directory containing the doc files
DOC_DIR="doc_descriptions/core"

# Function to remove duplicate doc blocks in a single file
remove_duplicates() {
    local file="$1"
    
    # Use awk to find and de-duplicate doc blocks
    awk '
    # Define regex patterns for start and end of doc blocks
    /^ *{% docs [^ ]+ %} *$/ { 
        block = ""; 
        start_line = $0; 
        in_block = 1; 
        next 
    } 

    /^ *{% enddocs %} *$/ && in_block { 
        block = block $0 RS; 
        block_hash = tolower(start_line); # Generate a hashable key
        if (!seen_blocks[block_hash]) { 
            seen_blocks[block_hash] = 1; 
            print start_line; 
            print block; 
        }
        in_block = 0; 
        next 
    } 

    in_block { 
        block = block $0 RS; 
        next 
    } 

    # Print lines outside of doc blocks as they are
    { print }
    ' "$file" > "$file.tmp" && mv "$file.tmp" "$file"
}

# Iterate over all .md files in the specified directory
find "$DOC_DIR" -type f -name "*.md" | while read -r file; do
    echo "Processing $file..."
    remove_duplicates "$file"
done

echo "De-duplication process completed."