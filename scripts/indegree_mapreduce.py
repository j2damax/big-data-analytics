#!/usr/bin/env python3
"""
Hadoop MapReduce: In-Degree Distribution Computation
Two-stage MapReduce implementation for calculating in-degree distribution in graphs

Stage 1: Calculate Individual Node In-Degrees
Stage 2: Calculate the Distribution
"""

from mrjob.job import MRJob
from mrjob.step import MRStep


class InDegreeDistribution(MRJob):
    """
    Two-stage MapReduce job to compute in-degree distribution of a graph
    
    Input format: Each line contains an edge "source destination"
    Output format: (in-degree, count) - number of nodes with that in-degree
    """

    def steps(self):
        """
        Define the two MapReduce stages
        """
        return [
            MRStep(mapper=self.mapper_indegree,
                   reducer=self.reducer_indegree),
            MRStep(mapper=self.mapper_distribution,
                   reducer=self.reducer_distribution)
        ]

    # ==================== STAGE 1: Calculate Individual Node In-Degrees ====================
    
    def mapper_indegree(self, _, line):
        """
        Stage 1 Mapper: Read each edge (u,v) and emit destination node v with count 1
        
        Args:
            line: Input line containing "source destination" or "source\tdestination"
        
        Yields:
            (destination_node, 1)
        """
        # Skip comment lines and empty lines
        line = line.strip()
        if not line or line.startswith('#'):
            return
        
        # Parse the edge (handle both space and tab delimiters)
        parts = line.split()
        if len(parts) >= 2:
            # source = parts[0]  # We don't need source for in-degree
            destination = parts[1]
            yield destination, 1

    def reducer_indegree(self, node, counts):
        """
        Stage 1 Reducer: Sum the counts for each destination node
        
        Args:
            node: The destination node
            counts: Iterator of 1s for each incoming edge
        
        Yields:
            (node, in-degree count)
        """
        yield node, sum(counts)

    # ==================== STAGE 2: Calculate the Distribution ====================
    
    def mapper_distribution(self, node, indegree):
        """
        Stage 2 Mapper: Transform (node, indegree) to (indegree, 1)
        
        Args:
            node: Node identifier (not used in output)
            indegree: The in-degree count for this node
        
        Yields:
            (indegree, 1)
        """
        yield indegree, 1

    def reducer_distribution(self, indegree, counts):
        """
        Stage 2 Reducer: Count how many nodes have each in-degree
        
        Args:
            indegree: The in-degree value
            counts: Iterator of 1s for each node with this in-degree
        
        Yields:
            (indegree, number of nodes with this in-degree)
        """
        yield indegree, sum(counts)


if __name__ == '__main__':
    InDegreeDistribution.run()
