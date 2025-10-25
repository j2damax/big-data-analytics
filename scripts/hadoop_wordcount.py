#!/usr/bin/env python3
"""
Hadoop MapReduce WordCount Example using mrjob
This script demonstrates basic Hadoop MapReduce operations
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")


class MRWordCount(MRJob):
    """
    A simple MapReduce job to count word frequencies in text
    """

    def mapper(self, _, line):
        """
        Map function: emit each word with count of 1
        Args:
            line: A line of text from input
        Yields:
            (word, 1) tuples
        """
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def reducer(self, word, counts):
        """
        Reduce function: sum up counts for each word
        Args:
            word: The word being counted
            counts: Iterator of counts for this word
        Yields:
            (word, total_count) tuples
        """
        yield word, sum(counts)


if __name__ == '__main__':
    MRWordCount.run()
