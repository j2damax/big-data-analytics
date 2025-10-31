#!/usr/bin/env python3
"""
Apache Spark: In-Degree Distribution Computation
Efficient in-memory computation using PySpark RDDs

This implementation calculates the in-degree distribution of a graph:
1. Read edge list data
2. Calculate in-degree for each node
3. Calculate distribution of in-degrees
"""

from pyspark import SparkContext, SparkConf
import sys
import time


class InDegreeSparkComputer:
    """
    Spark-based in-degree distribution calculator
    """
    
    def __init__(self, app_name="InDegreeDistribution"):
        """Initialize Spark context"""
        self.conf = SparkConf().setAppName(app_name)
        self.sc = SparkContext(conf=self.conf)
        self.sc.setLogLevel("WARN")  # Reduce logging verbosity
    
    def compute_indegree_distribution(self, input_path, output_path=None):
        """
        Compute in-degree distribution from edge list
        
        Args:
            input_path: Path to input file (local or HDFS)
            output_path: Optional path to save results
        
        Returns:
            List of (in-degree, count) tuples sorted by in-degree
        """
        start_time = time.time()
        
        # Step 1: Read and Transform - Extract destination nodes
        print(f"Reading edge list from: {input_path}")
        edges_rdd = self.sc.textFile(input_path)
        
        # Filter comments and empty lines, extract destination nodes
        destinations = edges_rdd \
            .filter(lambda line: line.strip() and not line.startswith('#')) \
            .map(lambda line: line.split()) \
            .filter(lambda parts: len(parts) >= 2) \
            .map(lambda parts: parts[1])  # Extract destination node
        
        # Step 2: Calculate Frequencies - Count incoming edges per node
        print("Calculating in-degrees for each node...")
        indegrees = destinations \
            .map(lambda node: (node, 1)) \
            .reduceByKey(lambda a, b: a + b)
        
        # Step 3: Calculate Distribution - Count nodes per in-degree
        print("Calculating in-degree distribution...")
        distribution = indegrees \
            .map(lambda node_count: (node_count[1], 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortByKey()  # Sort by in-degree for better readability
        
        # Collect results
        results = distribution.collect()
        
        elapsed_time = time.time() - start_time
        
        # Calculate statistics
        total_nodes = sum([count for _, count in results])
        total_edges = destinations.count()
        
        print(f"\n{'='*60}")
        print(f"In-Degree Distribution Computation Complete")
        print(f"{'='*60}")
        print(f"Total edges processed: {total_edges:,}")
        print(f"Total nodes with in-degree > 0: {total_nodes:,}")
        print(f"Unique in-degree values: {len(results):,}")
        print(f"Execution time: {elapsed_time:.2f} seconds")
        print(f"{'='*60}\n")
        
        # Save results if output path specified
        if output_path:
            print(f"Saving results to: {output_path}")
            distribution.saveAsTextFile(output_path)
        
        return results, {
            'execution_time': elapsed_time,
            'total_edges': total_edges,
            'total_nodes': total_nodes,
            'unique_indegrees': len(results)
        }
    
    def stop(self):
        """Stop Spark context"""
        self.sc.stop()


def main():
    """Main execution function"""
    if len(sys.argv) < 2:
        print("Usage: python3 indegree_spark.py <input_path> [output_path]")
        print("\nExample:")
        print("  python3 indegree_spark.py hdfs://hadoop:9000/user/root/snap_datasets/email-EuAll/email-EuAll.txt")
        print("  python3 indegree_spark.py /scripts/sample_data.txt /tmp/output")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Initialize and run computation
    computer = InDegreeSparkComputer()
    
    try:
        results, stats = computer.compute_indegree_distribution(input_path, output_path)
        
        # Display sample results
        print("Sample In-Degree Distribution (first 20 entries):")
        print(f"{'In-Degree':<15} {'Number of Nodes':<20}")
        print("-" * 40)
        for indegree, count in results[:20]:
            print(f"{indegree:<15} {count:<20,}")
        
        if len(results) > 20:
            print(f"... ({len(results) - 20} more entries)")
        
    finally:
        computer.stop()


if __name__ == "__main__":
    main()
