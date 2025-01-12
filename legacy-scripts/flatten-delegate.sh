#!/bin/bash

cd "$(dirname "$0")/.." || exit

src_dir="processed"
dest_dir="delegate"
combined_output=""

mkdir -p "$dest_dir"

if [[ $1 == "--combined" ]]; then
    combined_file="$dest_dir/$dest_dir-combined.json"
    combined_output="yes"
    echo "[]" > "$combined_file"
    shift
fi

process_json() {
    local input_file=$1
    local output_file=$2

    jq -r '[.[] |
       select(.transaction.messages[]? | 
           select(.["@type"] == "/cosmos.staking.v1beta1.MsgDelegate") // false) |
       .transaction |= (
           . + {
               "@type": ((.messages[]? | 
                   select(.["@type"] == "/cosmos.staking.v1beta1.MsgDelegate") | 
                   .["@type"]) // null),
               "amount": ((.messages[]? | 
                   select(.["@type"] == "/cosmos.staking.v1beta1.MsgDelegate") | 
                   .amount.amount | tonumber) / 1000000000),
               "from_address": (.messages[]? | 
                   select(.["@type"] == "/cosmos.staking.v1beta1.MsgDelegate") | 
                   .delegator_address),
               "to_address": (.messages[]? | 
                   select(.["@type"] == "/cosmos.staking.v1beta1.MsgDelegate") | 
                   .validator_address),
               "timestamp": (.timestamp | gsub("T"; " ") | split(":")[0:2] | join(":"))
           } | del(.messages, .logs)
       ) |
       . + .transaction |
       del(.transaction)]' "$input_file" > "$output_file"

    if [[ $combined_output == "yes" ]]; then
        jq -s '.[0] + .[1]' "$combined_file" "$output_file" > tmpfile && mv tmpfile "$combined_file"
    fi
}

for file in "$src_dir"/*.json; do
    base_name=$(basename "$file" .json)
    base_name=${base_name%"-$src_dir"}
    new_file="$dest_dir/${base_name}-${dest_dir}.json"
    process_json "$file" "$new_file"
done

echo "Processing complete. Files saved in $dest_dir"
[[ $combined_output == "yes" ]] && echo "Combined output saved to $combined_file"
