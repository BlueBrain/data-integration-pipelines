#!/bin/bash

# Function to URL-encode the type parameter
urlencode() {
    local encoded=""
    local length="${#1}"
    for (( i = 0; i < length; i++ )); do
        local char="${1:i:1}"
        case "$char" in
            [a-zA-Z0-9.~_-])
                encoded+="$char"
                ;;
            *)
                printf -v encoded_char '%%%02X' "'$char"
                encoded+="$encoded_char"
                ;;
        esac
    done
    echo "$encoded"
}

# Check if the required arguments were provided
if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ]; then
    echo "Usage: $0 <bearer_token> <from_param> <size_param> <output_file> <es_search_url> <type_param1,type_param2,...>"
    exit 1
fi

# Bearer token from the first argument
token="$1"

# From and size parameters from the second and third arguments
from_param="$2"
size_param="$3"

# Output CSV file from the fourth argument
output_file="$4"

# REST service URL
url="$5"

# Type parameters as a comma-separated list from the fifth argument
type_param="$6"
IFS=',' read -ra types <<< "$type_param"

# Build the JSON array for the types
types_json="["
for type in "${types[@]}"; do
    types_json+="\"$type\","
done
types_json="${types_json%,}]"

# Initialize CSV file with headers
echo "id,location,size" > "$output_file"

# Pagination variables
page_size=40
current_from=$from_param

# Loop through pages until all results are processed
while [ "$current_from" -lt "$((from_param + size_param))" ]; do
    # Adjust the size for the last page if necessary
    current_size=$((size_param - current_from + from_param))
    if [ "$current_size" -gt "$page_size" ]; then
        current_size=$page_size
    fi

    # Payload for the POST request
    payload=$(cat <<EOF
{
  "from": $current_from,
  "size": $current_size,
  "query": {
    "bool": {
      "filter": [
        {
          "terms": {
            "@type": $types_json
          }
        }
      ]
    }
  }
}
EOF
    )
    echo $payload
    echo "done payload"
    # Fetch the JSON response from the REST service with Authorization header
    response=$(curl -s -H "Authorization: Bearer $token" -H "Content-Type: application/json" -d "$payload" "$url")

    # Process the hits array using jq with --slurp
    echo "$response" | ./jq -c --slurp '.[].hits.hits[]' | while read -r hit; do
        # Extract id and _source._original_source
        id=$(echo "$hit" | ./jq -r '._source["@id"]')
        original_source=$(echo "$hit" | ./jq -r '._source._original_source')

        # Parse '_source._original_source' as JSON and extract 'distribution.atLocation.location'
        location=$(echo "$original_source" | ./jq -r '.distribution.atLocation.location')
        location=$(echo "$location" | sed 's|^file://||')

        # Check if the directory exists and get its size
        if [ -d "$location" ]; then
            size=$(du -sh "$location" | awk '{print $1}')
        else
            size="N/A"
        fi
        echo "Size of $location: $size"  
        # Append the results to the CSV file
        echo "$id,$location,$size" >> "$output_file"
    done

    # Increment the 'from' parameter to get the next page
    current_from=$((current_from + page_size))
done

