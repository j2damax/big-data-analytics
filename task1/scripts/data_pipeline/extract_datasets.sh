#!/bin/bash

# Extract all gzip files from data/raw to data/processed
mkdir -p data/processed

for file in data/raw/*.gz; do
    [ -f "$file" ] || continue
    output="data/processed/$(basename "$file" .gz | tr '[:upper:]' '[:lower:]')"
    [ -f "$output" ] && echo "Skipping $(basename "$file") (exists)" && continue
    echo "Extracting $(basename "$file")..."
    gunzip -c "$file" > "$output" && echo "✓ Done" || echo "✗ Failed"
done

echo "Extraction complete!"