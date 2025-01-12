#!/bin/bash

# Navigate to the directory containing your JSON files (if necessary)
# cd /path/to/directory

# Create an empty array to hold the processed JSON data from each file
output_array=()

# Iterate over each JSON file in the current directory
for file in *.json; do
    # Process the file with jq and add the output to the array
    # The revised jq command extracts the entire transaction object
    file_array=$(jq '[.data.messagesByAddress[]]' "$file")
    output_array+=("$file_array")
done

# Combine all the processed data into a single JSON array and write to combined.json
jq -s 'add | flatten' <<< "${output_array[*]}" > combined.json
