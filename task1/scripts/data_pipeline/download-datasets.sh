#!/bin/bash

# Simple dataset download script using curl commands

# Create data directory
mkdir -p data/raw

# Change to data directory
cd data/raw

# Download datasets using curl
echo "Downloading soc-pokec-relationships.txt.gz..."
curl -O https://snap.stanford.edu/data/soc-pokec-relationships.txt.gz

echo "Downloading email-EuAll.txt.gz..."
curl -O https://snap.stanford.edu/data/email-EuAll.txt.gz

echo "Downloading cit-Patents.txt.gz..."
curl -O https://snap.stanford.edu/data/cit-Patents.txt.gz

echo "Downloading soc-LiveJournal1.txt.gz..."
curl -O https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz

echo "All downloads completed!"