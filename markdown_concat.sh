#!/bin/zsh

# Check if the directory path is provided as an argument
if [ -z "$1" ]; then
    echo "Usage: $0 <directory_path>"
    exit 1
fi

# Path to the directory containing markdown files
dir_path="$1"

# Extract the final folder name and create the output file name
final_folder_name=$(basename "$dir_path")
output_file="$dir_path/complete_${final_folder_name}.md"

# Remove the output file if it already exists
rm -f "$output_file"

# Loop through all markdown files in the specified directory
for file in "$dir_path"/*.md; do
    # Check if the file exists and is a regular file
    if [[ -f "$file" ]]; then
        # Read the content of the current file, replace "docs eth_" with "docs evm_", and append to the output file
        sed 's/docs eth_/docs evm_/g' "$file" >> "$output_file"
        # Add a blank line after the content of the current file
        printf "\n\n" >> "$output_file"
    fi
done

echo "Markdown files concatenated into $output_file"