#!/bin/bash

# Navigate to the parent directory of the scripts folder
cd "$(dirname "$0")/.." || exit

# Define the source and destination directories relative to the parent directory
src_dir="raw"
dest_dir="processed"

# Create the destination directory if it doesn't exist
mkdir -p "$dest_dir"

# Function to process each file
process_file() {
    local input_file=$1
    local output_file=$2

    # Process the file with jq and write to a temporary file
    jq -r '[.data.messagesByAddress[] |
            .transaction |= (
                . + {
                    timestamp: (.block.timestamp | gsub("T"; " ") | split(":")[0:2] | join(":")),
                    height: .block.height,
                    fee_payer: .fee.payer,
                    fee_amount: ((.fee.amount[0].amount | tonumber) / 1000000000)
                } | del(.block, .fee)
            ) |
            select(.transaction.timestamp >= "2022-05-29 00:00")
           ]' "$input_file" > "$output_file"
}

# Loop over each file in the source directory
for file in "$src_dir"/*.json; do
    # Extract the base filename and replace the suffix
    base_name=$(basename "$file" .json)
    base_name=${base_name%"-$src_dir"}

    # Define the new filename with the destination directory suffixed
    new_file="$dest_dir/${base_name}-${dest_dir}.json"

    # Process the file
    process_file "$file" "$new_file"
done

echo "Processing complete. Files saved in $dest_dir"
