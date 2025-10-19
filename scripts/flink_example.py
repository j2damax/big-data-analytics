#!/usr/bin/env python3
"""
Apache Flink PyFlink Example
This script demonstrates basic Flink operations using PyFlink
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
import time


def datastream_wordcount_example():
    """
    Demonstrates Flink DataStream API with word count
    """
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Sample data
    data = [
        "Apache Flink is a framework for stateful computations",
        "Flink provides data distribution, communication, and fault tolerance",
        "Flink supports batch and stream processing"
    ]
    
    # Create DataStream
    ds = env.from_collection(collection=data)
    
    # Word count transformations
    def split_words(line):
        return line.lower().split()
    
    # Process the data
    word_counts = ds.flat_map(split_words) \
                    .map(lambda word: (word, 1)) \
                    .key_by(lambda x: x[0]) \
                    .reduce(lambda a, b: (a[0], a[1] + b[1]))
    
    # Print results
    word_counts.print()
    
    # Execute
    print("\n=== Flink DataStream WordCount Results ===")
    env.execute("Flink DataStream WordCount")


def table_api_example():
    """
    Demonstrates Flink Table API operations
    """
    # Create environments
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(environment_settings=env_settings)
    
    # Create a source table
    table_env.execute_sql("""
        CREATE TABLE technologies (
            name STRING,
            category STRING,
            year INT
        ) WITH (
            'connector' = 'datagen',
            'number-of-rows' = '10'
        )
    """)
    
    # Query the table
    result = table_env.sql_query("""
        SELECT category, COUNT(*) as count
        FROM technologies
        GROUP BY category
    """)
    
    print("\n=== Flink Table API Results ===")
    result.execute().print()


def streaming_example():
    """
    Demonstrates Flink streaming with Table API
    """
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(environment_settings=env_settings)
    
    # Create sample data
    data = [
        ("Hadoop", "Storage", 2006),
        ("Spark", "Processing", 2014),
        ("Kafka", "Messaging", 2011),
        ("Flink", "Stream Processing", 2015)
    ]
    
    # Create table from collection
    table = table_env.from_elements(data, ['technology', 'category', 'year'])
    
    # Perform operations
    result = table.select(col('technology'), col('category'), col('year')) \
                  .where(col('year') > 2010)
    
    print("\n=== Flink Streaming Table Results ===")
    result.execute().print()


if __name__ == "__main__":
    print("Starting Flink Examples...")
    
    try:
        print("\n1. DataStream API Example:")
        datastream_wordcount_example()
    except Exception as e:
        print(f"DataStream example error: {e}")
    
    print("\n" + "="*50)
    
    try:
        print("\n2. Streaming Table Example:")
        streaming_example()
    except Exception as e:
        print(f"Streaming example error: {e}")
    
    print("\nFlink Examples completed!")
