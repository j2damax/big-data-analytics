#!/usr/bin/env python3
"""
Hadoop MapReduce Implementation for In-Degree Distribution
Uses mrjob for simple MapReduce implementation compatible with Hadoop

In-degree: Number of incoming edges to a node (target node in directed graphs)
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import time


class MRInDegree(MRJob):
    """
    MapReduce job to compute in-degree distribution of graph nodes.
    
    Input format: Each line contains "source_node target_node"
    Output: Degree value and count (how many nodes have that in-degree)
    """
    
    def configure_args(self):
        """Add custom command line options"""
        super(MRInDegree, self).configure_args()
        self.add_passthru_arg(
            '--output-indegree',
            action='store_true',
            help='Output individual node in-degrees instead of distribution'
        )
    
    def steps(self):
        """Define the MapReduce steps"""
        if self.options.output_indegree:
            # Just compute in-degree for each node
            return [
                MRStep(
                    mapper=self.mapper_count_indegree,
                    combiner=self.reducer_sum_indegree,
                    reducer=self.reducer_sum_indegree
                )
            ]
        else:
            # Compute in-degree distribution
            return [
                MRStep(
                    mapper=self.mapper_count_indegree,
                    combiner=self.reducer_sum_indegree,
                    reducer=self.reducer_sum_indegree
                ),
                MRStep(
                    mapper=self.mapper_degree_distribution,
                    combiner=self.reducer_count_distribution,
                    reducer=self.reducer_count_distribution
                )
            ]
    
    def mapper_count_indegree(self, _, line):
        """
        Map function: Extract target nodes (in-degree receivers)
        
        Args:
            line: Input line in format "source target" or "source\ttarget"
        Yields:
            (target_node, 1) for each edge
        """
        # Skip comments and empty lines
        line = line.strip()
        if not line or line.startswith('#'):
            return
        
        # Parse edge: source -> target
        parts = line.split()
        if len(parts) >= 2:
            source = parts[0]
            target = parts[1]
            # Count incoming edge to target node
            yield target, 1
    
    def reducer_sum_indegree(self, node, counts):
        """
        Reduce function: Sum up in-degree for each node
        
        Args:
            node: Target node ID
            counts: Iterator of 1s representing incoming edges
        Yields:
            (node, total_indegree)
        """
        total_indegree = sum(counts)
        yield node, total_indegree
    
    def mapper_degree_distribution(self, node, indegree):
        """
        Map function for distribution: Group by degree value
        
        Args:
            node: Node ID
            indegree: In-degree of the node
        Yields:
            (indegree, 1) to count how many nodes have this degree
        """
        yield indegree, 1
    
    def reducer_count_distribution(self, degree, counts):
        """
        Reduce function for distribution: Count nodes per degree
        
        Args:
            degree: In-degree value
            counts: Iterator of 1s representing nodes with this degree
        Yields:
            (degree, count) - how many nodes have this in-degree
        """
        total_count = sum(counts)
        yield degree, total_count


if __name__ == '__main__':
    start_time = time.time()
    MRInDegree.run()
    end_time = time.time()
    # Execution time will be logged separately by the runner script
