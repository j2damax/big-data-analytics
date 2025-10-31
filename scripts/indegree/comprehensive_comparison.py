#!/usr/bin/env python3
"""
Comprehensive Performance Comparison Framework
Academic-grade comparison of Hadoop MapReduce vs Apache Spark for in-degree distribution analysis
"""

import os
import sys
import json
import time
import subprocess
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime

class PerformanceComparator:
    """
    Comprehensive framework for comparing Hadoop and Spark implementations
    """
    
    def __init__(self, output_dir="comparison_results"):
        self.output_dir = output_dir
        self.results = {}
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
    def run_hadoop_analysis(self, input_file, dataset_name):
        """Run Hadoop MapReduce analysis using unified tool"""
        print(f"\n🔄 Running Hadoop MapReduce Analysis for {dataset_name}")
        print("=" * 60)
        
        try:
            # Run unified tool with Hadoop method
            cmd = [
                "python3", "/scripts/indegree/indegree_analysis.py",
                input_file, "--method", "hadoop", "--save-results"
            ]
            
            start_time = time.time()
            result = subprocess.run(cmd, capture_output=True, text=True, cwd="/scripts/indegree")
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                # Parse output for statistics
                output_lines = result.stdout.strip().split('\n')
                hadoop_results = self._parse_unified_output(output_lines, execution_time)
                
                if hadoop_results:
                    return {
                        'framework': 'Hadoop MapReduce',
                        'execution_time': hadoop_results['execution_time'],
                        'total_nodes': hadoop_results['total_nodes'],
                        'max_indegree': hadoop_results['max_indegree'],
                        'unique_indegrees': hadoop_results['unique_indegrees'],
                        'distribution': hadoop_results['distribution']
                    }
            
            print(f"❌ Hadoop analysis failed with return code: {result.returncode}")
            if result.stderr:
                print(f"Error: {result.stderr}")
            return None
                
        except Exception as e:
            print(f"❌ Hadoop analysis failed: {str(e)}")
            return None
    
    def run_spark_analysis(self, input_file, dataset_name):
        """Run Apache Spark analysis using unified tool"""
        print(f"\n🔄 Running Apache Spark Analysis for {dataset_name}")
        print("=" * 60)
        
        try:
            # Run both RDD and DataFrame methods
            rdd_results = self._run_spark_method(input_file, "spark-rdd")
            dataframe_results = self._run_spark_method(input_file, "spark-dataframe")
            
            if rdd_results and dataframe_results:
                return {
                    'framework': 'Apache Spark',
                    'rdd_results': {
                        'execution_time': rdd_results['execution_time'],
                        'total_nodes': rdd_results['total_nodes'],
                        'max_indegree': rdd_results['max_indegree'],
                        'unique_indegrees': rdd_results['unique_indegrees'],
                        'distribution': rdd_results['distribution']
                    },
                    'dataframe_results': {
                        'execution_time': dataframe_results['execution_time'],
                        'total_nodes': dataframe_results['total_nodes'],
                        'max_indegree': dataframe_results['max_indegree'],
                        'unique_indegrees': dataframe_results['unique_indegrees'],
                        'distribution': dataframe_results['distribution']
                    }
                }
            else:
                return None
                
        except Exception as e:
            print(f"❌ Spark analysis failed: {str(e)}")
            return None
    
    def _run_spark_method(self, input_file, method):
        """Run specific Spark method using unified tool"""
        try:
            cmd = [
                "python3", "/scripts/indegree/indegree_analysis.py",
                input_file, "--method", method, "--save-results"
            ]
            
            start_time = time.time()
            result = subprocess.run(cmd, capture_output=True, text=True, cwd="/scripts/indegree")
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                output_lines = result.stdout.strip().split('\n')
                return self._parse_unified_output(output_lines, execution_time)
            
            print(f"❌ {method} analysis failed with return code: {result.returncode}")
            if result.stderr:
                print(f"Error: {result.stderr}")
            return None
            
        except Exception as e:
            print(f"❌ {method} analysis failed: {str(e)}")
            return None
    
    def _parse_unified_output(self, output_lines, execution_time):
        """Parse output from unified tool"""
        try:
            results = {
                'execution_time': execution_time,
                'total_nodes': 0,
                'max_indegree': 0,
                'unique_indegrees': 0,
                'distribution': {}
            }
            
            # Find statistics in output
            for line in output_lines:
                if "Total Nodes:" in line:
                    results['total_nodes'] = int(line.split(":")[1].strip().replace(",", ""))
                elif "Max In-Degree:" in line:
                    results['max_indegree'] = int(line.split(":")[1].strip().replace(",", ""))
                elif "Unique In-Degrees:" in line:
                    results['unique_indegrees'] = int(line.split(":")[1].strip().replace(",", ""))
            
            # Parse distribution from top 10 results
            in_table = False
            for line in output_lines:
                if "Top 10 In-Degrees:" in line:
                    in_table = True
                    continue
                elif in_table and line.strip() and not line.startswith("-"):
                    if line.strip().isdigit() or " " in line.strip():
                        parts = line.split()
                        if len(parts) >= 2 and parts[0].isdigit():
                            indegree = int(parts[0])
                            count = int(parts[1].replace(",", ""))
                            results['distribution'][indegree] = count
                elif in_table and (line.startswith("🌐") or line.startswith("✅")):
                    break
            
            return results
            
        except Exception as e:
            print(f"❌ Failed to parse output: {str(e)}")
            return None
    
    def compare_datasets(self, datasets):
        """
        Run comparative analysis across multiple datasets
        
        Args:
            datasets: List of tuples (file_path, dataset_name)
        """
        print(f"🎯 COMPREHENSIVE PERFORMANCE COMPARISON")
        print("=" * 80)
        print(f"📊 Analyzing {len(datasets)} datasets")
        print(f"🚀 Frameworks: Hadoop MapReduce vs Apache Spark (RDD + DataFrame)")
        print(f"📁 Results will be saved to: {self.output_dir}")
        
        comparison_results = []
        
        for i, (input_file, dataset_name) in enumerate(datasets, 1):
            print(f"\n" + "="*80)
            print(f"📈 DATASET {i}/{len(datasets)}: {dataset_name}")
            print("="*80)
            
            if not os.path.exists(input_file):
                print(f"❌ File not found: {input_file}")
                continue
            
            dataset_results = {
                'dataset_name': dataset_name,
                'input_file': input_file,
                'timestamp': datetime.now().isoformat()
            }
            
            # Run Hadoop analysis
            hadoop_results = self.run_hadoop_analysis(input_file, dataset_name)
            if hadoop_results:
                dataset_results['hadoop'] = hadoop_results
            
            # Run Spark analysis  
            spark_results = self.run_spark_analysis(input_file, dataset_name)
            if spark_results:
                dataset_results['spark'] = spark_results
            
            # Add to comparison results
            if hadoop_results or spark_results:
                comparison_results.append(dataset_results)
                
                # Print immediate comparison
                self.print_dataset_comparison(dataset_results)
        
        # Save comprehensive results
        self.save_comparison_results(comparison_results)
        
        # Generate visualizations
        self.generate_visualizations(comparison_results)
        
        # Print final summary
        self.print_final_summary(comparison_results)
        
        return comparison_results
    
    def print_dataset_comparison(self, dataset_results):
        """Print comparison results for a single dataset"""
        print(f"\n📊 PERFORMANCE COMPARISON: {dataset_results['dataset_name']}")
        print("-" * 60)
        
        # Extract results
        hadoop = dataset_results.get('hadoop')
        spark_rdd = dataset_results.get('spark', {}).get('rdd_results')
        spark_df = dataset_results.get('spark', {}).get('dataframe_results')
        
        if hadoop:
            print(f"Hadoop MapReduce:     {hadoop['execution_time']:.2f}s")
        
        if spark_rdd:
            print(f"Spark RDD:           {spark_rdd['execution_time']:.2f}s")
            
        if spark_df:
            print(f"Spark DataFrame:     {spark_df['execution_time']:.2f}s")
        
        # Calculate speedups
        if hadoop and spark_rdd:
            speedup = hadoop['execution_time'] / spark_rdd['execution_time']
            print(f"Spark RDD Speedup:    {speedup:.2f}x")
            
        if hadoop and spark_df:
            speedup = hadoop['execution_time'] / spark_df['execution_time']
            print(f"Spark DataFrame Speedup: {speedup:.2f}x")
    
    def generate_visualizations(self, comparison_results):
        """Generate performance comparison visualizations"""
        print(f"\n📊 Generating Performance Visualizations...")
        
        if not comparison_results:
            print("❌ No results to visualize")
            return
        
        # Prepare data for visualization
        datasets = []
        hadoop_times = []
        spark_rdd_times = []
        spark_df_times = []
        
        for result in comparison_results:
            dataset_name = result['dataset_name']
            datasets.append(dataset_name)
            
            # Extract execution times
            hadoop_time = result.get('hadoop', {}).get('execution_time', 0)
            spark_rdd_time = result.get('spark', {}).get('rdd_results', {}).get('execution_time', 0)
            spark_df_time = result.get('spark', {}).get('dataframe_results', {}).get('execution_time', 0)
            
            hadoop_times.append(hadoop_time)
            spark_rdd_times.append(spark_rdd_time)
            spark_df_times.append(spark_df_time)
        
        # Create performance comparison chart
        plt.figure(figsize=(15, 8))
        
        x = range(len(datasets))
        width = 0.25
        
        plt.bar([i - width for i in x], hadoop_times, width, label='Hadoop MapReduce', alpha=0.8)
        plt.bar(x, spark_rdd_times, width, label='Spark RDD', alpha=0.8)
        plt.bar([i + width for i in x], spark_df_times, width, label='Spark DataFrame', alpha=0.8)
        
        plt.xlabel('Datasets')
        plt.ylabel('Execution Time (seconds)')
        plt.title('Performance Comparison: Hadoop MapReduce vs Apache Spark\nIn-Degree Distribution Analysis')
        plt.xticks(x, datasets, rotation=45, ha='right')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        # Save plot
        plot_file = os.path.join(self.output_dir, f'performance_comparison_{self.timestamp}.png')
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"📊 Performance chart saved: {plot_file}")
        
        # Generate speedup analysis
        self.generate_speedup_analysis(comparison_results)
    
    def generate_speedup_analysis(self, comparison_results):
        """Generate speedup analysis visualization"""
        plt.figure(figsize=(12, 6))
        
        datasets = []
        rdd_speedups = []
        df_speedups = []
        
        for result in comparison_results:
            hadoop_time = result.get('hadoop', {}).get('execution_time')
            spark_rdd_time = result.get('spark', {}).get('rdd_results', {}).get('execution_time')
            spark_df_time = result.get('spark', {}).get('dataframe_results', {}).get('execution_time')
            
            if hadoop_time and spark_rdd_time and hadoop_time > 0:
                datasets.append(result['dataset_name'])
                rdd_speedups.append(hadoop_time / spark_rdd_time)
                df_speedups.append(hadoop_time / spark_df_time if spark_df_time else 0)
        
        if datasets:
            x = range(len(datasets))
            width = 0.35
            
            plt.bar([i - width/2 for i in x], rdd_speedups, width, label='Spark RDD vs Hadoop', alpha=0.8)
            plt.bar([i + width/2 for i in x], df_speedups, width, label='Spark DataFrame vs Hadoop', alpha=0.8)
            
            plt.xlabel('Datasets')
            plt.ylabel('Speedup Factor (Hadoop Time / Spark Time)')
            plt.title('Spark Performance Speedup Over Hadoop MapReduce')
            plt.xticks(x, datasets, rotation=45, ha='right')
            plt.axhline(y=1, color='r', linestyle='--', alpha=0.7, label='No Speedup (1x)')
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            # Save speedup plot
            speedup_file = os.path.join(self.output_dir, f'speedup_analysis_{self.timestamp}.png')
            plt.savefig(speedup_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"🚀 Speedup analysis saved: {speedup_file}")
    
    def save_comparison_results(self, comparison_results):
        """Save comprehensive comparison results to JSON"""
        results_file = os.path.join(self.output_dir, f'comprehensive_comparison_{self.timestamp}.json')
        
        summary = {
            'comparison_metadata': {
                'timestamp': datetime.now().isoformat(),
                'total_datasets': len(comparison_results),
                'frameworks_compared': ['Hadoop MapReduce', 'Apache Spark (RDD)', 'Apache Spark (DataFrame)']
            },
            'detailed_results': comparison_results
        }
        
        with open(results_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"📁 Comprehensive results saved: {results_file}")
    
    def print_final_summary(self, comparison_results):
        """Print final comprehensive summary"""
        print(f"\n" + "="*80)
        print(f"🏆 FINAL PERFORMANCE SUMMARY")
        print("="*80)
        
        if not comparison_results:
            print("❌ No successful comparisons completed")
            return
        
        total_datasets = len(comparison_results)
        hadoop_wins = 0
        spark_rdd_wins = 0
        spark_df_wins = 0
        
        avg_hadoop_time = 0
        avg_spark_rdd_time = 0
        avg_spark_df_time = 0
        
        valid_comparisons = 0
        
        for result in comparison_results:
            hadoop_time = result.get('hadoop', {}).get('execution_time')
            spark_rdd_time = result.get('spark', {}).get('rdd_results', {}).get('execution_time')
            spark_df_time = result.get('spark', {}).get('dataframe_results', {}).get('execution_time')
            
            if hadoop_time and spark_rdd_time and spark_df_time:
                valid_comparisons += 1
                
                avg_hadoop_time += hadoop_time
                avg_spark_rdd_time += spark_rdd_time
                avg_spark_df_time += spark_df_time
                
                # Determine winner
                min_time = min(hadoop_time, spark_rdd_time, spark_df_time)
                if hadoop_time == min_time:
                    hadoop_wins += 1
                elif spark_rdd_time == min_time:
                    spark_rdd_wins += 1
                else:
                    spark_df_wins += 1
        
        if valid_comparisons > 0:
            avg_hadoop_time /= valid_comparisons
            avg_spark_rdd_time /= valid_comparisons
            avg_spark_df_time /= valid_comparisons
            
            print(f"📊 Datasets Analyzed:     {total_datasets}")
            print(f"📊 Valid Comparisons:     {valid_comparisons}")
            print(f"")
            print(f"⏱️  Average Execution Times:")
            print(f"   Hadoop MapReduce:     {avg_hadoop_time:.2f}s")
            print(f"   Spark RDD:           {avg_spark_rdd_time:.2f}s")  
            print(f"   Spark DataFrame:     {avg_spark_df_time:.2f}s")
            print(f"")
            print(f"🏆 Performance Winners:")
            print(f"   Hadoop MapReduce:     {hadoop_wins}/{valid_comparisons}")
            print(f"   Spark RDD:           {spark_rdd_wins}/{valid_comparisons}")
            print(f"   Spark DataFrame:     {spark_df_wins}/{valid_comparisons}")
            
            # Overall speedup
            if avg_hadoop_time > 0:
                rdd_speedup = avg_hadoop_time / avg_spark_rdd_time
                df_speedup = avg_hadoop_time / avg_spark_df_time
                print(f"")
                print(f"🚀 Average Speedup vs Hadoop:")
                print(f"   Spark RDD:           {rdd_speedup:.2f}x")
                print(f"   Spark DataFrame:     {df_speedup:.2f}x")

def main():
    """Main execution function for comprehensive comparison"""
    if len(sys.argv) < 3 or len(sys.argv) % 2 == 0:
        print("Usage: python3 comprehensive_comparison.py <file1> <name1> [file2] [name2] ...")
        print("\nExample:")
        print("  python3 comprehensive_comparison.py \\")
        print("    data/processed/email-EuAll.txt email-EuAll \\")
        print("    data/processed/cit-Patents.txt patents \\")
        print("    data/processed/soc-LiveJournal1.txt livejournal")
        sys.exit(1)
    
    # Parse datasets
    datasets = []
    for i in range(1, len(sys.argv), 2):
        input_file = sys.argv[i]
        dataset_name = sys.argv[i + 1]
        datasets.append((input_file, dataset_name))
    
    print(f"🎯 COMPREHENSIVE HADOOP VS SPARK PERFORMANCE COMPARISON")
    print("="*80)
    print(f"📊 Framework Comparison: Hadoop MapReduce vs Apache Spark")
    print(f"🔬 Analysis Type: In-Degree Distribution")
    print(f"📈 Datasets: {len(datasets)}")
    
    # Create comparator and run analysis
    comparator = PerformanceComparator("comprehensive_results")
    results = comparator.compare_datasets(datasets)
    
    if results:
        print(f"\n✅ Comprehensive comparison completed!")
        print(f"📁 All results saved in: comprehensive_results/")
        print(f"\n🌐 Monitor live performance:")
        print(f"   Hadoop YARN:    http://localhost:8088")
        print(f"   Hadoop HDFS:    http://localhost:9870")
        print(f"   Spark Master:   http://localhost:8080")
    else:
        print(f"❌ Comparison failed")
        sys.exit(1)

if __name__ == '__main__':
    main()