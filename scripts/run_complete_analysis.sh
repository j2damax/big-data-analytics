#!/bin/bash
##
# Complete In-Degree Distribution Analysis Pipeline
# This script runs the full analysis workflow:
# 1. Run experiments on datasets (MapReduce + Spark)
# 2. Generate visualizations
# 3. Create comprehensive report
# 4. Package deliverables
##

set -e  # Exit on error

echo "=========================================="
echo "In-Degree Distribution Analysis Pipeline"
echo "=========================================="
echo ""

# Check if datasets specified
DATASETS="$@"
if [ -z "$DATASETS" ]; then
    echo "Running on all available datasets..."
    echo "To run on specific datasets, use: $0 email-EuAll cit-Patents"
else
    echo "Running on specified datasets: $DATASETS"
fi

echo ""
echo "Step 1: Running experiments..."
echo "----------------------------------------"

# Run experiments
python3 /scripts/run_indegree_experiments.py $DATASETS

if [ $? -ne 0 ]; then
    echo "ERROR: Experiments failed!"
    exit 1
fi

echo ""
echo "Step 2: Generating visualizations and report..."
echo "----------------------------------------"

# Generate analysis
python3 /scripts/analyze_indegree_results.py

if [ $? -ne 0 ]; then
    echo "ERROR: Analysis generation failed!"
    exit 1
fi

echo ""
echo "Step 3: Packaging deliverables..."
echo "----------------------------------------"

# Create deliverables directory
DELIVERABLES_DIR="/tmp/indegree_deliverables_$(date +%s)"
mkdir -p "$DELIVERABLES_DIR"

# Copy source files
echo "Copying source files..."
cp /scripts/indegree_mapreduce.py "$DELIVERABLES_DIR/"
cp /scripts/indegree_spark.py "$DELIVERABLES_DIR/"
cp /scripts/run_indegree_experiments.py "$DELIVERABLES_DIR/"
cp /scripts/analyze_indegree_results.py "$DELIVERABLES_DIR/"
cp /scripts/run_complete_analysis.sh "$DELIVERABLES_DIR/"

# Copy results and plots
echo "Copying results and visualizations..."
if [ -d "/tmp/indegree_results" ]; then
    cp -r /tmp/indegree_results "$DELIVERABLES_DIR/"
fi

if [ -d "/tmp/indegree_plots" ]; then
    cp -r /tmp/indegree_plots "$DELIVERABLES_DIR/"
fi

# Create ZIP archive
echo "Creating ZIP archive..."
cd /tmp
ZIP_FILE="indegree_analysis_deliverables_$(date +%Y%m%d_%H%M%S).zip"
zip -r "$ZIP_FILE" "$(basename $DELIVERABLES_DIR)" > /dev/null

echo ""
echo "=========================================="
echo "Analysis Pipeline Complete!"
echo "=========================================="
echo ""
echo "Deliverables:"
echo "  - Source files: $DELIVERABLES_DIR/"
echo "  - ZIP archive:  /tmp/$ZIP_FILE"
echo ""
echo "Key files:"
echo "  - Report: /tmp/indegree_plots/IN_DEGREE_ANALYSIS_REPORT.md"
echo "  - Results: /tmp/indegree_results/"
echo "  - Plots: /tmp/indegree_plots/"
echo ""
echo "To download the deliverables, copy the ZIP file:"
echo "  docker cp <container>:/tmp/$ZIP_FILE ."
echo ""
