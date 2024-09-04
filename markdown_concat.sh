#!/bin/zsh

# Path to the directory containing markdown files
dir_path="doc_descriptions/defi/dex"

# Output file
output_file="/Users/mattromano/Desktop/repos/fsc-evm/doc_descriptions//defi/complete_dex.md"

# Remove the output file if it already exists
rm -f "$output_file"

# Loop through all markdown files in the specified directory
for file in "$dir_path"/*.md; do
    # Check if the file exists and is a regular file
    if [[ -f "$file" ]]; then
        # Concatenate the content of the current file to the output file
        cat "$file" >> "$output_file"
        # Add a blank line after the content of the current file
        printf "\n\n" >> "$output_file"
    fi
done

echo "Markdown files concatenated into $output_file"