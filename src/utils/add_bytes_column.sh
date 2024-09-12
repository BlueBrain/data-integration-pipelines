#!/bin/bash

# Function to convert human-readable size to bytes, handling decimal values
convert_to_bytes() {
    size=$1
    if [[ $size =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        # Size is in bytes (no suffix)
        echo "$size"
    elif [[ $size =~ ^([0-9]+(\.[0-9]+)?)([KMGTP])B?$ ]]; then
        number=${BASH_REMATCH[1]}
        unit=${BASH_REMATCH[3]}
        case $unit in
            K) echo "$(echo "$number * 1024" | bc)" ;;
            M) echo "$(echo "$number * 1024 * 1024" | bc)" ;;
            G) echo "$(echo "$number * 1024 * 1024 * 1024" | bc)" ;;
            T) echo "$(echo "$number * 1024 * 1024 * 1024 * 1024" | bc)" ;;
            P) echo "$(echo "$number * 1024 * 1024 * 1024 * 1024 * 1024" | bc)" ;;
        esac
    else
        echo "0"
    fi
}

# Check if the required argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <input_csv_file>"
    exit 1
fi

input_file="$1"
output_file="location_to_size_with_bytes.csv"


# Read the CSV file and process each line
{
    # Read the header
    IFS=, read -r header
    # Print the header with the new 'size_in_bytes' column
    echo "$header,size_in_bytes"
    
    # Read the rest of the lines
    while IFS=, read -r id location size; do
        # Convert the 'size' to bytes
        size_in_bytes=$(convert_to_bytes "$size")
        # Print the line with the new 'size_in_bytes' column
        echo "$id,$location,$size,$size_in_bytes"
    done
} < "$input_file" > "$output_file"

echo "Processed CSV saved to $output_file"

