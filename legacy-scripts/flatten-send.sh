#!/bin/bash

# Navigate to the parent directory of the scripts folder
cd "$(dirname "$0")/.." || exit

# Define the source and destination directories relative to the parent directory
src_dir="processed"
dest_dir="send"
combined_output=""

# Create the destination directory if it doesn't exist
mkdir -p "$dest_dir"

# Check for the flag to create a combined file
if [[ $1 == "--combined" ]]; then
    combined_file="$dest_dir/$dest_dir-combined.json"
    combined_output="yes"
    # Start the combined file as an empty array
    echo "[]" > "$combined_file"
    shift
fi

# Function to process the JSON file
process_json() {
    local input_file=$1
    local output_file=$2

    jq -r '[.[] | 
       select(.transaction.messages[].["@type"] == "/cosmos.bank.v1beta1.MsgSend") |
       . += {
           "@type": (.transaction.messages[] | select(.["@type"] == "/cosmos.bank.v1beta1.MsgSend") | .["@type"]),
           "amount": ((.transaction.messages[] | select(.["@type"] == "/cosmos.bank.v1beta1.MsgSend") | .amount[0].amount | tonumber) / 1000000000),
           "from_address": (.transaction.messages[] | select(.["@type"] == "/cosmos.bank.v1beta1.MsgSend") | .from_address),
           "to_address": (.transaction.messages[] | select(.["@type"] == "/cosmos.bank.v1beta1.MsgSend") | .to_address)
       } |
       del(.transaction.messages, .transaction.logs) |
       . += .transaction |
       del(.transaction)
    ]' "$input_file" > "$output_file"

    # Append to the combined file if the flag is set
    if [[ $combined_output == "yes" ]]; then
        # Merge the current file's JSON array with the existing array in the combined file
        jq -s '.[0] + .[1]' "$combined_file" "$output_file" > tmpfile && mv tmpfile "$combined_file"
    fi
}

# Loop over each file in the source directory
for file in "$src_dir"/*.json; do
    # Extract the base filename and replace the suffix
    base_name=$(basename "$file" .json)
    base_name=${base_name%"-$src_dir"}

    # Define the new filename with the destination directory suffixed
    new_file="$dest_dir/${base_name}-${dest_dir}.json"

    # Process the file
    process_json "$file" "$new_file"
done

echo "Processing complete. Files saved in $dest_dir"
[[ $combined_output == "yes" ]] && echo "Combined output saved to $combined_file"
