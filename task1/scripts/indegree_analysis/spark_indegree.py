#!/usr/bin/env python3
"""
Apache Spark Implementation for In-Degree Distribution
Uses PySpark for distributed in-memory processing

In-degree: Number of incoming edges to a node (target node in directed graphs)
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import time
import sys
import argparse


class SparkInDegree:
    """Spark-based in-degree computation and distribution analysis"""
    
    def __init__(self, input_path, output_path=None, output_indegree=False):
        """
        Initialize Spark session
        
        Args:
            input_path: Path to input graph data (HDFS or local)
            output_path: Path to save output results
            output_indegree: If True, output node in-degrees; else output distribution
        """
        self.input_path = input_path
        self.output_path = output_path
        self.output_indegree = output_indegree
        
        # Create Spark configuration
        conf = SparkConf().setAppName("Spark-InDegree-Analysis")
        
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        
        self.sc = self.spark.sparkContext
        # Set log level to reduce output
        self.sc.setLogLevel("WARN")
    
    def compute_indegree(self):
        """
        Compute in-degree for each node using Spark RDD operations
        
        Returns:
            RDD of (node, indegree) tuples
        """
        # Read input file
        lines = self.sc.textFile(self.input_path)
        
        # Filter comments and empty lines, parse edges
        edges = lines.filter(lambda line: line.strip() and not line.startswith('#')) \
                    .map(lambda line: line.strip().split()) \
                    .filter(lambda parts: len(parts) >= 2) \
                    .map(lambda parts: (parts[0], parts[1]))  # (source, target)
        
        # Count in-degree: map target nodes to 1, then reduce by key
        indegrees = edges.map(lambda edge: (edge[1], 1)) \
                        .reduceByKey(lambda a, b: a + b) \
                        .sortBy(lambda x: x[1], ascending=False)
        
        return indegrees
    
    def compute_distribution(self, indegrees):
        """
        Compute in-degree distribution from node in-degrees
        
        Args:
            indegrees: RDD of (node, indegree) tuples
            
        Returns:
            RDD of (degree, count) tuples sorted by degree
        """
        # Group by degree value and count
        distribution = indegrees.map(lambda x: (x[1], 1)) \
                               .reduceByKey(lambda a, b: a + b) \
                               .sortByKey()
        
        return distribution
    
    def run(self):
        """
        Execute the in-degree analysis
        
        Returns:
            dict with results and statistics
        """
        start_time = time.time()
        
        # Compute in-degrees
        indegrees = self.compute_indegree()
        
        # Cache for reuse
        indegrees.cache()
        
        if self.output_indegree:
            # Output individual node in-degrees
            results = indegrees.collect()
            
            # Save or print results
            if self.output_path:
                indegrees.saveAsTextFile(self.output_path)
                print(f"Node in-degrees saved to: {self.output_path}")
            
            # Calculate statistics
            total_nodes = indegrees.count()
            max_indegree = indegrees.map(lambda x: x[1]).max() if total_nodes > 0 else 0
            avg_indegree = indegrees.map(lambda x: x[1]).mean() if total_nodes > 0 else 0
            
        else:
            # Compute and output distribution
            distribution = self.compute_distribution(indegrees)
            results = distribution.collect()
            
            # Save or print results
            if self.output_path:
                distribution.saveAsTextFile(self.output_path)
                print(f"In-degree distribution saved to: {self.output_path}")
            
            # Calculate statistics
            total_nodes = indegrees.count()
            max_indegree = indegrees.map(lambda x: x[1]).max() if total_nodes > 0 else 0
            avg_indegree = indegrees.map(lambda x: x[1]).mean() if total_nodes > 0 else 0
            total_degree_classes = distribution.count()
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Print statistics
        print("\n" + "="*60)
        print("Spark In-Degree Analysis Results")
        print("="*60)
        print(f"Input file: {self.input_path}")
        print(f"Total nodes with in-degree > 0: {total_nodes}")
        print(f"Maximum in-degree: {max_indegree}")
        print(f"Average in-degree: {avg_indegree:.2f}")
        if not self.output_indegree:
            print(f"Number of unique degree values: {total_degree_classes}")
        print(f"Execution time: {execution_time:.2f} seconds")
        print("="*60)
        
        # Print sample results
        if not self.output_indegree:
            print("\nSample distribution (degree -> count):")
            for degree, count in results[:10]:
                print(f"  In-degree {degree}: {count} nodes")
            if len(results) > 10:
                print(f"  ... and {len(results) - 10} more degree values")
        else:
            print("\nTop 10 nodes by in-degree:")
            for node, degree in results[:10]:
                print(f"  Node {node}: in-degree = {degree}")
        
        print("")
        
        return {
            'execution_time': execution_time,
            'total_nodes': total_nodes,
            'max_indegree': max_indegree,
            'avg_indegree': avg_indegree,
            'results': results
        }
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()


def main():
    """Main function to run Spark in-degree analysis"""
    parser = argparse.ArgumentParser(
        description='Compute in-degree distribution using Apache Spark'
    )
    parser.add_argument(
        'input',
        help='Input graph file path (HDFS or local)'
    )
    parser.add_argument(
        '--output',
        help='Output path for results (optional)',
        default=None
    )
    parser.add_argument(
        '--output-indegree',
        action='store_true',
        help='Output individual node in-degrees instead of distribution'
    )
    
    args = parser.parse_args()
    
    # Run analysis
    analyzer = SparkInDegree(
        args.input,
        args.output,
        args.output_indegree
    )
    
    try:
        results = analyzer.run()
    finally:
        analyzer.stop()


if __name__ == '__main__':
    main()
