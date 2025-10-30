#!/bin/bash
# Demo script for In-Degree Distribution MapReduce computation
# This script demonstrates the usage with sample graph datasets

set -euo pipefail

echo "=========================================="
echo "In-Degree Distribution Demo"
echo "=========================================="
echo ""

# Create a sample graph dataset
echo "Step 1: Creating sample graph datasets..."
echo ""

# Simple test graph
cat > /tmp/simple_graph.txt << 'EOF'
# Simple directed graph
# Edges: source -> destination
1 2
1 3
2 3
3 4
4 5
EOF

# Star topology graph
cat > /tmp/star_graph.txt << 'EOF'
# Star topology with node 100 as center
1 100
2 100
3 100
4 100
5 100
6 100
7 100
8 100
9 100
10 100
EOF

# Complex graph with various patterns
cat > /tmp/complex_graph.txt << 'EOF'
# Complex graph demonstrating various patterns
# Social network connections (multiple people follow person 50)
1 50
2 50
3 50
4 50
5 50
10 50
11 50
12 50
13 50
14 50
# Linear chain
20 21
21 22
22 23
23 24
24 25
# Bidirectional edges (mutual connections)
30 31
31 30
32 33
33 32
# Triangle
40 41
41 42
42 40
# Hub with various connections
60 70
61 70
62 70
70 80
70 81
70 82
EOF

echo "✓ Sample datasets created:"
echo "  - /tmp/simple_graph.txt (5 edges)"
echo "  - /tmp/star_graph.txt (10 edges)"
echo "  - /tmp/complex_graph.txt (22 edges)"
echo ""

# Run MapReduce on each dataset
echo "=========================================="
echo "Step 2: Running MapReduce jobs..."
echo "=========================================="
echo ""

# Test 1: Simple graph
echo "Test 1: Simple Graph"
echo "-------------------"
echo "Input graph:"
cat /tmp/simple_graph.txt | grep -v "^#"
echo ""
echo "Running MapReduce..."
python3 scripts/hadoop_indegree.py /tmp/simple_graph.txt 2>/dev/null
echo ""
echo "Interpretation:"
echo "  Node 2: in-degree 1 (from 1)"
echo "  Node 3: in-degree 2 (from 1, 2)"
echo "  Node 4: in-degree 1 (from 3)"
echo "  Node 5: in-degree 1 (from 4)"
echo "  → Result: 3 nodes with in-degree 1, 1 node with in-degree 2"
echo ""

# Test 2: Star graph
echo "Test 2: Star Topology"
echo "--------------------"
echo "Input: 10 nodes all pointing to node 100"
echo ""
echo "Running MapReduce..."
python3 scripts/hadoop_indegree.py /tmp/star_graph.txt 2>/dev/null
echo ""
echo "Interpretation:"
echo "  - 1 node (node 100) has in-degree 10"
echo ""

# Test 3: Complex graph
echo "Test 3: Complex Graph"
echo "--------------------"
echo "Input: Multiple patterns (hub, chain, triangle, mutual)"
echo ""
echo "Running MapReduce..."
python3 scripts/hadoop_indegree.py /tmp/complex_graph.txt 2>/dev/null
echo ""
echo "Interpretation:"
echo "  Shows distribution across various network patterns"
echo ""

# Visualization
echo "=========================================="
echo "Step 3: Understanding the Output"
echo "=========================================="
echo ""
echo "Output Format: <in-degree> <count>"
echo ""
echo "Example output '10  1' means:"
echo "  → 1 node has 10 incoming edges"
echo ""
echo "Example output '2  5' means:"
echo "  → 5 nodes each have 2 incoming edges"
echo ""

echo "=========================================="
echo "Next Steps"
echo "=========================================="
echo ""
echo "To run on real HDFS datasets:"
echo ""
echo "1. Start Hadoop container:"
echo "   make hadoop"
echo ""
echo "2. Run on email-EuAll dataset (420K edges):"
echo "   make test-hadoop-indegree"
echo ""
echo "3. Run on other datasets:"
echo "   docker exec hadoop python3 /scripts/hadoop_indegree.py \\"
echo "       hdfs:///user/root/snap_datasets/cit-Patents/cit-Patents.txt"
echo ""
echo "4. Save output to file:"
echo "   make test-hadoop-indegree > indegree_results.txt"
echo ""
echo "For more information, see: INDEGREE_DISTRIBUTION_GUIDE.md"
echo ""
