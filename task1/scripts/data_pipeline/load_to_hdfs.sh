#!/bin/bash
# Simple HDFS loader script - loads processed data files to HDFS

# Configuration
PROCESSED_DIR="/data/processed"
HDFS_BASE_PATH="/user/root/snap_datasets"
REPLICATION=3
export HADOOP_ROOT_LOGGER="ERROR,console"


# Test HDFS connection
echo "Testing HDFS connection..."
if ! hadoop fs -ls / &> /dev/null; then
    echo "Error: Cannot connect to HDFS"
    exit 1
fi

# Create base directory
echo "Creating HDFS base directory: $HDFS_BASE_PATH"
hadoop fs -mkdir -p "$HDFS_BASE_PATH"

# Find and upload all .txt files
echo "Uploading processed files to HDFS..."
for file in "$PROCESSED_DIR"/*.txt; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        dataset_name="${filename%.txt}"
        hdfs_dir="$HDFS_BASE_PATH/$dataset_name"
        hdfs_path="$hdfs_dir/$filename"
        
        echo "Uploading: $filename"
        hadoop fs -mkdir -p "$hdfs_dir"
        hadoop fs -put -f "$file" "$hdfs_path"
        hadoop fs -setrep "$REPLICATION" "$hdfs_path"
        
        echo "✓ Uploaded: $filename"
    fi
done

echo "Upload complete. Files in HDFS:"
hadoop fs -ls -R "$HDFS_BASE_PATH"