#!/bin/bash

cd "$(dirname "$0")/.." || exit

src_dir="processed"
dest_dir="reward"
standalone_output="no"
combined_file="$dest_dir/$dest_dir-combined.json"

mkdir -p "$dest_dir"
echo "[]" > "$combined_file"

if [[ $1 == "--standalone" ]]; then
    standalone_output="yes"
fi

# Function to process the JSON file with initial transformation
transform_json() {
    local input_file=$1
    local output_file=$2

    jq -r '[.[] | 
    select(.transaction.messages[].["@type"] == "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward") |
    .transaction.amount = (
        .transaction.logs[] |
        .events[] |
        select(.type == "coin_received" and any(.attributes[]; .key == "receiver" and .value == "cheqd1pav86kd046tv7em6nc2dgcycxdppdnyzu2xycv")) |
        .attributes[] |
        select(.key == "amount") |
        select(.value | type == "string" and endswith("ncheq")) |
        .value | rtrimstr("ncheq") | tonumber
    ) | 
    .transaction.amount /= 1000000000 |
    .transaction["@type"] = (.transaction.messages[0]."@type") |
    del(.transaction.messages, .transaction.logs) |
    . += .transaction |
    del(.transaction)
    ]' "$input_file" > "$output_file"

    # Append to the combined file
    jq -s '.[0] + .[1]' "$combined_file" "$output_file" > tmpfile && mv tmpfile "$combined_file"
}

for file in "$src_dir"/*.json; do
    base_name=$(basename "$file" .json)
    base_name=${base_name%"-$src_dir"}

    new_file="$dest_dir/${base_name}-${dest_dir}.json"

    transform_json "$file" "$new_file"

    if [[ $standalone_output == "no" ]]; then
        # Optional: Remove the standalone file if standalone output is not requested
        rm "$new_file"
    fi
done

# Process the combined file for uniqueness
jq -r 'unique_by(.hash, .success, .timestamp, .height, .fee_payer, .fee_amount, .amount, ."@type")' "$combined_file" > tmpfile && mv tmpfile "$combined_file"

# Process the combined file for amount aggregation
jq -r '[group_by(.hash, .success, .timestamp, .height, .fee_payer, .fee_amount, ."@type")[] |
    {
        hash: .[0].hash,
        success: .[0].success,
        timestamp: .[0].timestamp,
        height: .[0].height,
        fee_payer: .[0].fee_payer,
        fee_amount: .[0].fee_amount,
        "@type": .[0]."@type",
        amount: (map(.amount | select(type == "number")) | add)
    }
]' "$combined_file" > tmpfile && mv tmpfile "$combined_file"

echo "Processing complete. Combined file saved in $dest_dir"
[[ $standalone_output == "yes" ]] && echo "Standalone files also saved in $dest_dir"
