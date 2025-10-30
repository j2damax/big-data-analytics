#!/usr/bin/env python3
"""
Hadoop MapReduce In-Degree Distribution Computation using mrjob

This script implements a two-stage MapReduce pipeline to compute in-degree 
distribution for directed graph datasets:
- Stage 1: Calculate individual node in-degrees
- Stage 2: Calculate the distribution of in-degrees

Input format: Each line represents a directed edge as "source destination"
Output format: (In-Degree k, Number of Nodes with In-Degree k)
"""

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRInDegreeDistribution(MRJob):
    """
    Two-stage MapReduce job to compute in-degree distribution of a directed graph
    """

    def steps(self):
        """
        Define the two-stage MapReduce pipeline
        """
        return [
            MRStep(mapper=self.mapper_get_destinations,
                   reducer=self.reducer_count_indegree),
            MRStep(mapper=self.mapper_group_by_indegree,
                   reducer=self.reducer_count_distribution)
        ]

    # Stage 1: Calculate Individual Node In-Degrees
    
    def mapper_get_destinations(self, _, line):
        """
        Stage 1 Mapper: Extract destination nodes from edges
        
        Reads each edge (u,v) and outputs the destination node v with value 1.
        This allows us to count how many edges point to each node.
        
        Args:
            line: A line from input file in format "source destination"
        
        Yields:
            (destination_node, 1) tuples
        """
        # Skip comment lines that start with #
        if line.startswith('#'):
            return
        
        # Parse the edge: split on whitespace/tab
        parts = line.strip().split()
        
        # Valid edge should have exactly 2 nodes
        if len(parts) == 2:
            source, destination = parts
            # Emit destination node with count 1
            yield destination, 1

    def reducer_count_indegree(self, node, counts):
        """
        Stage 1 Reducer: Sum the in-degree for each node
        
        Receives all edges pointing to a node and sums them to get the in-degree.
        
        Args:
            node: The destination node
            counts: Iterator of 1s (one for each incoming edge)
        
        Yields:
            (node, in_degree_count) tuples
        """
        in_degree = sum(counts)
        yield node, in_degree

    # Stage 2: Calculate the Distribution
    
    def mapper_group_by_indegree(self, node, in_degree):
        """
        Stage 2 Mapper: Group nodes by their in-degree
        
        Takes the output from Stage 1 and emits the in-degree as key.
        This groups all nodes with the same in-degree together.
        
        Args:
            node: The node identifier
            in_degree: The in-degree count for this node
        
        Yields:
            (in_degree, 1) tuples
        """
        # Emit in-degree as key with count 1
        yield in_degree, 1

    def reducer_count_distribution(self, in_degree, counts):
        """
        Stage 2 Reducer: Count how many nodes have each in-degree
        
        Sums up the number of nodes for each in-degree value.
        
        Args:
            in_degree: The in-degree value (k)
            counts: Iterator of 1s (one for each node with this in-degree)
        
        Yields:
            (in_degree, node_count) tuples - Final output
        """
        node_count = sum(counts)
        yield in_degree, node_count


if __name__ == '__main__':
    MRInDegreeDistribution.run()
