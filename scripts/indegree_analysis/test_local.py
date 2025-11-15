#!/usr/bin/env python3
"""
Local test script for in-degree implementations
Tests both Hadoop and Spark implementations with sample data
"""

import os
import sys
import subprocess
import tempfile

def create_sample_data():
    """Create a small sample graph for testing"""
    # Create a simple directed graph
    # Node 1 -> 2, 3, 4
    # Node 2 -> 3
    # Node 3 -> 4
    # Node 4 -> 2
    # Expected in-degrees: {2:2, 3:2, 4:2} (nodes 1 has 0)
    
    sample_data = """# Sample directed graph
1 2
1 3
1 4
2 3
3 4
4 2
"""
    
    # Create temporary file
    fd, path = tempfile.mkstemp(suffix='.txt', prefix='test_graph_')
    with os.fdopen(fd, 'w') as f:
        f.write(sample_data)
    
    return path

def test_hadoop_local(input_file):
    """Test Hadoop implementation locally"""
    print("\n" + "="*60)
    print("Testing Hadoop MapReduce Implementation (Local Mode)")
    print("="*60)
    
    try:
        cmd = [
            'python3',
            '/home/runner/work/big-data-analytics/big-data-analytics/scripts/indegree_analysis/hadoop_indegree.py',
            input_file
        ]
        
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        print("\nOutput:")
        print(result.stdout)
        
        if result.returncode != 0:
            print("\nErrors:")
            print(result.stderr)
            return False
        
        # Verify results
        lines = result.stdout.strip().split('\n')
        indegrees = {}
        for line in lines:
            if '\t' in line:
                parts = line.split('\t')
                if len(parts) == 2:
                    try:
                        degree = int(parts[0].strip('"'))
                        count = int(parts[1])
                        indegrees[degree] = count
                    except ValueError:
                        continue
        
        print(f"\nParsed distribution: {indegrees}")
        
        # Expected: degree 2 appears 3 times (nodes 2, 3, 4 each have in-degree 2)
        if 2 in indegrees and indegrees[2] == 3:
            print("✓ Test PASSED - Hadoop implementation correct")
            return True
        else:
            print(f"✗ Test FAILED - Expected {{2: 3}}, got {indegrees}")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ Test FAILED - Timeout")
        return False
    except Exception as e:
        print(f"✗ Test FAILED - Error: {str(e)}")
        return False

def test_spark_local(input_file):
    """Test Spark implementation locally (if Spark available)"""
    print("\n" + "="*60)
    print("Testing Apache Spark Implementation (Local Mode)")
    print("="*60)
    
    # Check if spark-submit is available
    try:
        subprocess.run(['which', 'spark-submit'], 
                      capture_output=True, 
                      check=True)
    except:
        print("⚠ Spark not available in PATH, skipping Spark test")
        print("  (This is expected if not running inside Spark container)")
        return None
    
    try:
        cmd = [
            'spark-submit',
            '--master', 'local[1]',
            '/home/runner/work/big-data-analytics/big-data-analytics/scripts/indegree_analysis/spark_indegree.py',
            input_file
        ]
        
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        print("\nOutput:")
        # Print last 1000 chars to avoid too much Spark logging
        print(result.stdout[-1000:])
        
        if result.returncode != 0:
            print("\nErrors:")
            print(result.stderr[-1000:])
            return False
        
        # Look for statistics in output
        if "Total nodes" in result.stdout and "Maximum in-degree" in result.stdout:
            print("✓ Test PASSED - Spark implementation executed successfully")
            return True
        else:
            print("✗ Test FAILED - Could not verify output")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ Test FAILED - Timeout")
        return False
    except Exception as e:
        print(f"✗ Test FAILED - Error: {str(e)}")
        return False

def main():
    """Run all tests"""
    print("\n" + "#"*60)
    print("# In-Degree Distribution - Local Tests")
    print("#"*60)
    
    # Create sample data
    print("\nCreating sample graph data...")
    sample_file = create_sample_data()
    print(f"Sample file created: {sample_file}")
    
    with open(sample_file, 'r') as f:
        print("\nSample data:")
        print(f.read())
    
    # Run tests
    results = {}
    
    # Test Hadoop
    results['hadoop'] = test_hadoop_local(sample_file)
    
    # Test Spark
    results['spark'] = test_spark_local(sample_file)
    
    # Clean up
    os.remove(sample_file)
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Hadoop MapReduce: {'✓ PASSED' if results['hadoop'] else '✗ FAILED'}")
    if results['spark'] is not None:
        print(f"Apache Spark:     {'✓ PASSED' if results['spark'] else '✗ FAILED'}")
    else:
        print(f"Apache Spark:     ⚠ SKIPPED (not available)")
    print("="*60)
    
    # Exit code
    if results['hadoop'] and (results['spark'] is None or results['spark']):
        print("\n✓ All available tests passed!")
        return 0
    else:
        print("\n✗ Some tests failed")
        return 1

if __name__ == '__main__':
    sys.exit(main())
