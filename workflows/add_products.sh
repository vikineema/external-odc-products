#!/bin/bash

# Check if a directory containing products is provided as an argument
if [[ -z "$1" ]]; then
    echo "Usage: $0 <products_directory>"
    exit 1
fi

# Directory containing metadata files
PRODUCTS_DIR="$1"

if [[ ! -d "$PRODUCTS_DIR" ]]; then
    echo "Directory $PRODUCTS_DIR does not exist. Exiting."
    exit 1
fi

# Loop over each file in the products directory
shopt -s nullglob  # Avoids executing loop if no matches
for product_file in "$PRODUCTS_DIR"/*.odc-product.yaml; do
  # Check if the file is a regular file
  if [[ -f "$product_file" ]]; then
    echo "Adding product from file: $product_file"
    # Run the datacube product add command
    datacube product add "$product_file"
  else
    echo "Skipping non-regular file: $product_file"
  fi
done