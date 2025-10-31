#!/usr/bin/env python3
"""
In-Degree Distribution Analysis Tool
Academic-grade implementation supporting multiple execution methods:
- Pure Python (baseline)
- Hadoop MapReduce (distributed batch processing)
- Apache Spark RDD (distributed in-memory processing)
- Apache Spark DataFrame (SQL-optimized processing)
"""

import sys
import time
import os
import json
import argparse
from collections import Counter, defaultdict

# Framework availability checks
HADOOP_AVAILABLE = False
SPARK_AVAILABLE = False

# Try to import framework dependencies
try:
    from mrjob.job import MRJob
    from mrjob.step import MRStep
    HADOOP_AVAILABLE = True
except ImportError:
    # Create dummy base class for when MRJob is not available
    class MRJob:
        def __init__(self, *args, **kwargs):
            pass

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, desc
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

class PerformanceMonitor:
    """Universal performance monitoring for all methods"""
    
    def __init__(self, method_name):
        self.method_name = method_name
        self.start_time = None
        self.end_time = None
        
    def start(self):
        """Start performance monitoring"""
        self.start_time = time.time()
        print(f"🚀 Starting {self.method_name} analysis...")
        
    def stop(self):
        """Stop monitoring and return metrics"""
        self.end_time = time.time()
        execution_time = self.end_time - self.start_time if self.start_time else 0
        print(f"✅ {self.method_name} completed in {execution_time:.2f}s")
        
        return {
            'method': self.method_name,
            'execution_time': execution_time,
            'start_time': self.start_time,
            'end_time': self.end_time,
        }

class PythonInDegreeAnalyzer:
    """Pure Python implementation - baseline method"""
    
    @staticmethod
    def analyze(input_file):
        """Pure Python in-degree distribution analysis"""
        print("🔄 Using Pure Python method...")
        
        # Stage 1: Count in-degrees
        indegrees = defaultdict(int)
        
        with open(input_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split()
                    if len(parts) >= 2:
                        destination = parts[1]
                        indegrees[destination] += 1
        
        # Stage 2: Calculate distribution
        distribution = Counter(indegrees.values())
        
        return {
            'distribution': dict(distribution),
            'total_nodes': len(indegrees),
            'max_indegree': max(indegrees.values()) if indegrees else 0,
            'unique_indegrees': len(distribution)
        }

class HadoopInDegreeAnalyzer(MRJob):
    """Hadoop MapReduce implementation"""
    
    def configure_args(self):
        """Configure command line arguments"""
        super(HadoopInDegreeAnalyzer, self).configure_args()
        self.add_passthru_arg('--dataset-name', default='unknown',
                             help='Name of the dataset for reporting')
    
    def steps(self):
        """Define the MapReduce pipeline steps"""
        return [
            MRStep(mapper=self.mapper_count_indegrees,
                   reducer=self.reducer_count_indegrees),
            MRStep(mapper=self.mapper_distribution,
                   reducer=self.reducer_distribution)
        ]
    
    def mapper_count_indegrees(self, _, line):
        """Stage 1 Mapper: Extract destination nodes from edges"""
        line = line.strip()
        if line and not line.startswith('#'):
            parts = line.split()
            if len(parts) >= 2:
                yield parts[1], 1  # (destination, 1)
    
    def reducer_count_indegrees(self, destination, counts):
        """Stage 1 Reducer: Sum up in-degree counts for each node"""
        yield destination, sum(counts)
    
    def mapper_distribution(self, destination, indegree):
        """Stage 2 Mapper: Transform to (indegree, count) pairs"""
        yield indegree, 1
    
    def reducer_distribution(self, indegree, counts):
        """Stage 2 Reducer: Count nodes for each in-degree value"""
        yield indegree, sum(counts)

class SparkInDegreeAnalyzer:
    """Apache Spark implementation with RDD and DataFrame methods"""
    
    def __init__(self, app_name="UnifiedInDegreeAnalysis"):
        self.app_name = app_name
        self.spark = None
        self.sc = None
        
    def initialize(self):
        """Initialize Spark session"""
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            self.sc = self.spark.sparkContext
            self.sc.setLogLevel("WARN")
            
            print(f"✅ Spark Session initialized: {self.spark.version}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to initialize Spark: {str(e)}")
            return False
    
    def analyze_with_rdd(self, input_file):
        """RDD-based implementation"""
        print("🔄 Using Spark RDD method...")
        
        # Load and process data
        lines_rdd = self.sc.textFile(input_file)
        
        # Filter and parse edges
        edges_rdd = lines_rdd \
            .filter(lambda line: line.strip() and not line.strip().startswith('#')) \
            .map(lambda line: line.strip().split()) \
            .filter(lambda parts: len(parts) >= 2) \
            .map(lambda parts: (parts[0], parts[1]))
        
        # Count in-degrees
        indegrees_rdd = edges_rdd \
            .map(lambda edge: (edge[1], 1)) \
            .reduceByKey(lambda a, b: a + b)
        
        # Create distribution
        distribution_rdd = indegrees_rdd \
            .map(lambda node_indegree: (node_indegree[1], 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .sortByKey()
        
        # Collect results
        results = dict(distribution_rdd.collect())
        total_nodes = indegrees_rdd.count()
        max_indegree = indegrees_rdd.map(lambda x: x[1]).max() if total_nodes > 0 else 0
        
        return {
            'distribution': results,
            'total_nodes': total_nodes,
            'max_indegree': max_indegree,
            'unique_indegrees': len(results)
        }
    
    def analyze_with_dataframe(self, input_file):
        """DataFrame-based implementation"""
        print("🔄 Using Spark DataFrame method...")
        
        # Load data as DataFrame
        raw_df = self.spark.read.text(input_file)
        
        # Parse edges
        edges_df = raw_df \
            .filter(~col("value").startswith("#")) \
            .filter(col("value").rlike(r"^\s*\S+\s+\S+")) \
            .selectExpr("split(trim(value), '\\\\s+')[0] as source",
                       "split(trim(value), '\\\\s+')[1] as destination") \
            .filter(col("source").isNotNull() & col("destination").isNotNull())
        
        # Calculate in-degrees
        indegrees_df = edges_df \
            .groupBy("destination") \
            .agg(count("*").alias("indegree"))
        
        # Create distribution
        distribution_df = indegrees_df \
            .groupBy("indegree") \
            .agg(count("*").alias("node_count")) \
            .orderBy("indegree")
        
        # Collect results
        results = {row.indegree: row.node_count for row in distribution_df.collect()}
        
        # Calculate statistics
        from pyspark.sql.functions import max as spark_max
        stats = indegrees_df.agg(
            count("*").alias("total_nodes"),
            spark_max("indegree").alias("max_indegree")
        ).collect()[0]
        
        return {
            'distribution': results,
            'total_nodes': stats.total_nodes,
            'max_indegree': stats.max_indegree,
            'unique_indegrees': len(results)
        }
    
    def close(self):
        """Close Spark session"""
        if self.spark:
            self.spark.stop()

class InDegreeAnalysis:
    """Main analysis class supporting multiple frameworks"""
    
    def __init__(self):
        self.results = {}
        
    def run_python_analysis(self, input_file, dataset_name):
        """Run pure Python analysis"""
        monitor = PerformanceMonitor("Pure Python")
        monitor.start()
        
        try:
            results = PythonInDegreeAnalyzer.analyze(input_file)
            performance = monitor.stop()
            
            return {
                **results,
                'performance': performance,
                'dataset_name': dataset_name,
                'framework': 'Pure Python'
            }
            
        except Exception as e:
            print(f"❌ Python analysis failed: {str(e)}")
            return None
    
    def run_hadoop_analysis(self, input_file, dataset_name):
        """Run Hadoop MapReduce analysis"""
        if not HADOOP_AVAILABLE:
            print("❌ Hadoop analysis unavailable: mrjob not installed")
            return None
            
        monitor = PerformanceMonitor("Hadoop MapReduce")
        monitor.start()
        
        try:
            # Create and run MapReduce job
            job = HadoopInDegreeAnalyzer(args=[
                input_file,
                '--dataset-name', dataset_name
            ])
            
            # Collect results directly from job
            results = {}
            total_nodes = 0
            max_indegree = 0
            
            with job.make_runner() as runner:
                runner.run()
                
                for line in runner.cat_output():
                    # Parse mrjob output format: "key"\t"value"
                    line_str = line.decode('utf-8') if isinstance(line, bytes) else line
                    line_str = line_str.strip()
                    
                    if '\t' in line_str:
                        key_str, value_str = line_str.split('\t', 1)
                        # Remove quotes if present
                        key_str = key_str.strip('"')
                        value_str = value_str.strip('"')
                        
                        try:
                            indegree = int(key_str)
                            node_count = int(value_str)
                            
                            results[indegree] = node_count
                            total_nodes += node_count
                            max_indegree = max(max_indegree, indegree)
                        except ValueError:
                            # Skip invalid lines
                            continue
            
            performance = monitor.stop()
            
            return {
                'distribution': results,
                'total_nodes': total_nodes,
                'max_indegree': max_indegree,
                'unique_indegrees': len(results),
                'performance': performance,
                'dataset_name': dataset_name,
                'framework': 'Hadoop MapReduce'
            }
            
        except Exception as e:
            print(f"❌ Hadoop analysis failed: {str(e)}")
            return None
    
    def run_spark_analysis(self, input_file, dataset_name, spark_method='both'):
        """Run Apache Spark analysis"""
        if not SPARK_AVAILABLE:
            print("❌ Spark analysis unavailable: pyspark not installed")
            return None
        
        analyzer = SparkInDegreeAnalyzer(f"InDegreeAnalysis-{dataset_name}")
        
        if not analyzer.initialize():
            return None
        
        results = {}
        
        try:
            # RDD analysis
            if spark_method in ['rdd', 'both']:
                monitor = PerformanceMonitor("Spark RDD")
                monitor.start()
                
                rdd_results = analyzer.analyze_with_rdd(input_file)
                rdd_performance = monitor.stop()
                
                results['rdd'] = {
                    **rdd_results,
                    'performance': rdd_performance,
                    'dataset_name': dataset_name,
                    'framework': 'Apache Spark (RDD)'
                }
            
            # DataFrame analysis
            if spark_method in ['dataframe', 'both']:
                monitor = PerformanceMonitor("Spark DataFrame")
                monitor.start()
                
                df_results = analyzer.analyze_with_dataframe(input_file)
                df_performance = monitor.stop()
                
                results['dataframe'] = {
                    **df_results,
                    'performance': df_performance,
                    'dataset_name': dataset_name,
                    'framework': 'Apache Spark (DataFrame)'
                }
            
            return results
            
        except Exception as e:
            print(f"❌ Spark analysis failed: {str(e)}")
            return None
            
        finally:
            analyzer.close()
    
    def print_results_summary(self, results, method_name):
        """Print formatted results summary"""
        if not results:
            print(f"❌ No results for {method_name}")
            return
        
        print(f"\n📊 {method_name.upper()} RESULTS:")
        print("=" * 50)
        print(f"Execution Time:    {results['performance']['execution_time']:.2f}s")
        print(f"Total Nodes:       {results['total_nodes']:,}")
        print(f"Max In-Degree:     {results['max_indegree']:,}")
        print(f"Unique In-Degrees: {results['unique_indegrees']:,}")
        
        # Show top in-degrees
        distribution = results['distribution']
        sorted_items = sorted(distribution.items(), key=lambda x: int(x[0]))
        
        print(f"\nTop 10 In-Degrees:")
        print(f"{'In-Degree':<12} {'Node Count':<12}")
        print("-" * 25)
        
        for i, (indegree, count) in enumerate(sorted_items[:10]):
            print(f"{indegree:<12} {count:<12,}")
    
    def save_results(self, results, output_file):
        """Save results to JSON file"""
        try:
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"📁 Results saved to: {output_file}")
        except Exception as e:
            print(f"❌ Failed to save results: {str(e)}")

def main():
    """Main execution function with argument parsing"""
    parser = argparse.ArgumentParser(
        description="In-Degree Distribution Analysis Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Pure Python analysis
  python3 indegree_analysis.py data/email-EuAll.txt --method python
  
  # Hadoop MapReduce analysis
  python3 indegree_analysis.py data/email-EuAll.txt --method hadoop
  
  # Spark RDD analysis
  python3 indegree_analysis.py data/email-EuAll.txt --method spark-rdd
  
  # Spark DataFrame analysis  
  python3 indegree_analysis.py data/email-EuAll.txt --method spark-dataframe
  
  # Run all methods for comparison
  python3 indegree_analysis.py data/email-EuAll.txt --method all
        """
    )
    
    parser.add_argument('input_file', help='Input graph file path')
    parser.add_argument('--method', 
                       choices=['python', 'hadoop', 'spark-rdd', 'spark-dataframe', 'spark-both', 'all'],
                       default='python',
                       help='Analysis method to use (default: python)')
    parser.add_argument('--dataset-name', 
                       help='Dataset name for reporting (default: filename)')
    parser.add_argument('--output-dir', default='results',
                       help='Output directory for results (default: results)')
    parser.add_argument('--save-results', action='store_true',
                       help='Save results to JSON file')
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.input_file):
        print(f"❌ Error: Input file '{args.input_file}' not found")
        sys.exit(1)
    
    # Set dataset name
    dataset_name = args.dataset_name or os.path.basename(args.input_file).replace('.txt', '')
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Initialize analyzer
    analyzer = InDegreeAnalysis()
    
    print("🎯 IN-DEGREE DISTRIBUTION ANALYSIS")
    print("=" * 60)
    print(f"Dataset: {dataset_name}")
    print(f"Input: {args.input_file}")
    print(f"Method: {args.method}")
    
    all_results = {}
    
    # Run analysis based on method
    if args.method == 'python' or args.method == 'all':
        result = analyzer.run_python_analysis(args.input_file, dataset_name)
        if result:
            all_results['python'] = result
            analyzer.print_results_summary(result, 'Python')
    
    if args.method == 'hadoop' or args.method == 'all':
        result = analyzer.run_hadoop_analysis(args.input_file, dataset_name)
        if result:
            all_results['hadoop'] = result
            analyzer.print_results_summary(result, 'Hadoop MapReduce')
    
    if args.method in ['spark-rdd', 'spark-dataframe', 'spark-both'] or args.method == 'all':
        spark_method = 'both' if args.method in ['spark-both', 'all'] else args.method.replace('spark-', '')
        results = analyzer.run_spark_analysis(args.input_file, dataset_name, spark_method)
        
        if results:
            if 'rdd' in results:
                all_results['spark-rdd'] = results['rdd']
                analyzer.print_results_summary(results['rdd'], 'Spark RDD')
                
            if 'dataframe' in results:
                all_results['spark-dataframe'] = results['dataframe']
                analyzer.print_results_summary(results['dataframe'], 'Spark DataFrame')
    
    # Performance comparison summary
    if len(all_results) > 1:
        print(f"\n🏆 PERFORMANCE COMPARISON SUMMARY:")
        print("=" * 60)
        
        execution_times = []
        for method, result in all_results.items():
            exec_time = result['performance']['execution_time']
            execution_times.append((method, exec_time))
            print(f"{method:<15}: {exec_time:>8.2f}s")
        
        # Find fastest method
        fastest = min(execution_times, key=lambda x: x[1])
        print(f"\n🚀 Fastest method: {fastest[0]} ({fastest[1]:.2f}s)")
    
    # Save results if requested
    if args.save_results and all_results:
        output_file = os.path.join(args.output_dir, f"{dataset_name}_unified_results.json")
        analyzer.save_results(all_results, output_file)
    
    # Show monitoring URLs
    print(f"\n🌐 Monitor distributed processing:")
    print(f"   Hadoop YARN: http://localhost:8088")
    print(f"   Spark UI:    http://localhost:8080")
    
    print(f"\n✅ Analysis complete!")

if __name__ == '__main__':
    main()