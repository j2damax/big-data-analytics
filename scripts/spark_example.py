#!/usr/bin/env python3
"""
Spark PySpark Example - WordCount
This script demonstrates basic Spark operations using PySpark
"""

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


def spark_wordcount_example():
    """
    Demonstrates Spark WordCount using RDD API
    """
    # Initialize Spark
    conf = SparkConf().setAppName("SparkWordCount")
    sc = SparkContext(conf=conf)
    
    # Sample data
    data = [
        "Apache Spark is a unified analytics engine",
        "Spark provides high-level APIs in Java, Scala, Python and R",
        "Spark supports SQL queries, streaming data, machine learning and graph processing"
    ]
    
    # Create RDD
    rdd = sc.parallelize(data)
    
    # Word count operations
    word_counts = (rdd
                  .flatMap(lambda line: line.lower().split())
                  .map(lambda word: (word, 1))
                  .reduceByKey(lambda a, b: a + b)
                  .sortBy(lambda x: x[1], ascending=False))
    
    # Collect and print results
    print("\n=== Spark WordCount Results ===")
    for word, count in word_counts.collect():
        print(f"{word}: {count}")
    
    sc.stop()


def spark_dataframe_example():
    """
    Demonstrates Spark DataFrame operations
    """
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("SparkDataFrameExample") \
        .getOrCreate()
    
    # Sample data
    data = [
        ("Hadoop", "Big Data", 2006),
        ("Spark", "Big Data", 2014),
        ("Kafka", "Streaming", 2011),
        ("Flink", "Streaming", 2015)
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(data, ["Technology", "Category", "Year"])
    
    print("\n=== Spark DataFrame Example ===")
    df.show()
    
    # SQL operations
    df.createOrReplaceTempView("technologies")
    result = spark.sql("SELECT Category, COUNT(*) as count FROM technologies GROUP BY Category")
    
    print("\n=== Technologies by Category ===")
    result.show()
    
    spark.stop()


if __name__ == "__main__":
    print("Starting Spark Examples...")
    spark_wordcount_example()
    print("\n" + "="*50 + "\n")
    spark_dataframe_example()
    print("\nSpark Examples completed!")
