#!/usr/bin/env python3
"""
Experiment Runner for In-Degree Distribution Analysis
Runs either Hadoop or Spark implementation on multiple datasets and collects metrics
"""

import subprocess
import time
import json
import os
import sys
from datetime import datetime
import argparse


class ExperimentRunner:
    """Run in-degree experiments and collect performance metrics"""
    
    def __init__(self, datasets, output_dir="results", framework=None):
        """
        Initialize experiment runner
        
        Args:
            datasets: List of dataset names to process
            output_dir: Directory to save results
            framework: Framework to run ('spark' or 'hadoop')
        """
        self.datasets = datasets
        self.output_dir = output_dir
        self.framework = framework
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Load existing results or create new structure
        self.results_file = os.path.join(output_dir, 'experiment_results.json')
        self.results = self.load_existing_results()
    
    def load_existing_results(self):
        """
        Load existing results from JSON file if it exists
        
        Returns:
            Dictionary containing existing results or new structure
        """
        if os.path.exists(self.results_file):
            try:
                with open(self.results_file, 'r') as f:
                    existing_results = json.load(f)
                print(f"Loaded existing results from: {self.results_file}")
                print(f"Found {len(existing_results.get('experiments', []))} existing experiments")
                return existing_results
            except Exception as e:
                print(f"Warning: Could not load existing results: {e}")
                print("Creating new results structure")
        
        return {
            'timestamp': datetime.now().isoformat(),
            'experiments': []
        }
    
    def update_results_for_framework(self, new_metrics):
        """
        Update results by replacing experiments for the current framework
        
        Args:
            new_metrics: New experiment metrics to add/update
        """
        dataset_name = new_metrics['dataset']
        framework_name = new_metrics['framework']
        
        # Remove any existing experiment with same dataset and framework
        self.results['experiments'] = [
            exp for exp in self.results['experiments'] 
            if not (exp.get('dataset') == dataset_name and exp.get('framework') == framework_name)
        ]
        
        # Add the new experiment
        self.results['experiments'].append(new_metrics)
        
        # Update timestamp
        self.results['timestamp'] = datetime.now().isoformat()
    
    def remove_hdfs_output(self, output_path):
        """
        Remove existing output directory in HDFS
        
        Args:
            output_path: Path to output directory in HDFS
        """
        try:
            cmd = ['hadoop', 'fs', '-rm', '-r', '-f', output_path]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"Removed existing output directory: {output_path}")
            else:
                print(f"Output directory did not exist or could not be removed: {output_path}")
        except Exception as e:
            print(f"Warning: Could not remove output directory {output_path}: {str(e)}")
    
    def read_hdfs_results(self, output_path):
        """
        Read and parse results from HDFS output directory
        
        Args:
            output_path: HDFS path to the output directory
            
        Returns:
            dict with parsed statistics or None if failed
        """
        try:
            # Read the output file (part-00000 is the standard output file name)
            result_file = f"{output_path}/part-00000"
            cmd = ['hadoop', 'fs', '-cat', result_file]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                print(f"Failed to read HDFS results from {result_file}")
                return None
            
            # Parse the output to extract statistics
            lines = result.stdout.strip().split('\n')
            degree_distribution = {}
            total_nodes = 0
            max_degree = 0
            total_degree_sum = 0
            
            for line in lines:
                if line.strip():
                    parts = line.strip().split('\t')
                    if len(parts) == 2:
                        try:
                            degree = int(parts[0])
                            count = int(parts[1])
                            degree_distribution[degree] = count
                            total_nodes += count
                            max_degree = max(max_degree, degree)
                            total_degree_sum += degree * count
                        except ValueError:
                            continue
            
            if total_nodes == 0:
                return None
            
            avg_degree = total_degree_sum / total_nodes if total_nodes > 0 else 0
            unique_degrees = len(degree_distribution)
            
            return {
                'total_nodes': total_nodes,
                'max_indegree': max_degree,
                'avg_indegree': round(avg_degree, 2),
                'unique_degrees': unique_degrees,
                'degree_distribution': degree_distribution
            }
            
        except Exception as e:
            print(f"Error reading HDFS results: {str(e)}")
            return None
    
    def run_hadoop_mapreduce(self, dataset_name, input_path):
        """
        Run Hadoop MapReduce in-degree analysis
        
        Args:
            dataset_name: Name of the dataset
            input_path: Path to input file in HDFS
            
        Returns:
            dict with performance metrics
        """
        print(f"\n{'='*60}")
        print(f"Running Hadoop MapReduce on {dataset_name}")
        print(f"{'='*60}")
        
        output_path = f"hdfs://hadoop:9000/user/root/output/hadoop_{dataset_name}_distribution"
        indegree_path = f"{self.output_dir}/hadoop_{dataset_name}_indegree"
        
        # Remove existing output files
        self.remove_hdfs_output(output_path)
        
        start_time = time.time()
        
        try:
            # Run MapReduce job for distribution
            cmd = [
                'python3',
                '/scripts/indegree_analysis/hadoop_indegree.py',
                '-r', 'hadoop',
                '--hadoop-streaming-jar', '/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar',
                input_path,
                '--output-dir', output_path
            ]
            
            print(f"Command: {' '.join(cmd)}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=1800  # 30 minute timeout
            )
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Parse output for statistics
            success = result.returncode == 0
            
            metrics = {
                'framework': 'Hadoop MapReduce',
                'optimization': 'Combiner enabled for local aggregation',
                'dataset': dataset_name,
                'input_path': input_path,
                'output_path': output_path,
                'execution_time': execution_time,
                'success': success,
                'stdout': result.stdout[-2000:] if result.stdout else '',  # Last 2000 chars
                'stderr': result.stderr[-2000:] if result.stderr else ''
            }
            
            if success:
                print(f"[OK] Hadoop job completed successfully in {execution_time:.2f} seconds")
            else:
                print(f"[x] Hadoop job failed")
                print(f"Error: {result.stderr[:500]}")
            
            # If job succeeded, read and parse results from HDFS
            if success:
                print(f"[OK] Hadoop job completed successfully in {execution_time:.2f} seconds")
                print("Reading results from HDFS...")
                
                hdfs_results = self.read_hdfs_results(output_path)
                if hdfs_results:
                    metrics.update(hdfs_results)
                    print(f"✓ Parsed results: {hdfs_results['total_nodes']} nodes, "
                          f"max degree: {hdfs_results['max_indegree']}, "
                          f"avg degree: {hdfs_results['avg_indegree']}")
                else:
                    print("⚠ Could not parse results from HDFS output")
            else:
                print(f"[x] Hadoop job failed")
                print(f"Error: {result.stderr[:500]}")
            
            return metrics
            
        except subprocess.TimeoutExpired:
            print(f"[x] Hadoop job timed out after 30 minutes")
            return {
                'framework': 'Hadoop MapReduce',
                'dataset': dataset_name,
                'execution_time': 1800,
                'success': False,
                'error': 'Timeout'
            }
        except Exception as e:
            print(f"[x] Error running Hadoop job: {str(e)}")
            return {
                'framework': 'Hadoop MapReduce',
                'dataset': dataset_name,
                'success': False,
                'error': str(e)
            }
    
    def run_spark(self, dataset_name, input_path):
        """
        Run Spark in-degree analysis
        
        Args:
            dataset_name: Name of the dataset
            input_path: Path to input file
            
        Returns:
            dict with performance metrics
        """
        print(f"\n{'='*60}")
        print(f"Running Apache Spark on {dataset_name}")
        print(f"{'='*60}")
        
        output_path = f"hdfs://hadoop:9000/user/root/output/spark_{dataset_name}_distribution"
        
        # Remove existing output files
        self.remove_hdfs_output(output_path)
        
        start_time = time.time()
        
        try:
            # Run Spark job
            cmd = [
                'spark-submit',
                '--master', 'spark://spark-master:7077',
                '/scripts/indegree_analysis/spark_indegree.py',
                input_path,
                '--output', output_path
            ]
            
            print(f"Command: {' '.join(cmd)}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=1800  # 30 minute timeout
            )
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            success = result.returncode == 0
            
            # Parse output for statistics
            metrics = {
                'framework': 'Apache Spark',
                'optimization': 'Reduced shuffle partitions to 64',
                'dataset': dataset_name,
                'input_path': input_path,
                'output_path': output_path,
                'execution_time': execution_time,
                'success': success,
                'stdout': result.stdout[-2000:] if result.stdout else '',
                'stderr': result.stderr[-2000:] if result.stderr else ''
            }
            
            # Extract statistics from Spark output
            if success and result.stdout:
                try:
                    for line in result.stdout.split('\n'):
                        if 'Total nodes' in line:
                            metrics['total_nodes'] = int(line.split(':')[1].strip())
                        elif 'Maximum in-degree' in line:
                            metrics['max_indegree'] = int(line.split(':')[1].strip())
                        elif 'Average in-degree' in line:
                            metrics['avg_indegree'] = float(line.split(':')[1].strip())
                        elif 'Number of unique degree values' in line:
                            metrics['unique_degrees'] = int(line.split(':')[1].strip())
                except:
                    pass
            
            if success:
                print(f"[OK] Spark job completed successfully in {execution_time:.2f} seconds")
                print("Reading degree distribution from HDFS...")
                
                # Read and parse degree distribution from HDFS output
                hdfs_results = self.read_hdfs_results(output_path)
                if hdfs_results:
                    metrics.update(hdfs_results)
                    print(f"✓ Parsed degree distribution: {hdfs_results['unique_degrees']} unique degrees")
                else:
                    print("⚠ Could not parse degree distribution from HDFS output")
            else:
                print(f"[x] Spark job failed")
                print(f"Error: {result.stderr[:500]}")
            
            return metrics
            
        except subprocess.TimeoutExpired:
            print(f"[x] Spark job timed out after 30 minutes")
            return {
                'framework': 'Apache Spark',
                'dataset': dataset_name,
                'execution_time': 1800,
                'success': False,
                'error': 'Timeout'
            }
        except Exception as e:
            print(f"[x] Error running Spark job: {str(e)}")
            return {
                'framework': 'Apache Spark',
                'dataset': dataset_name,
                'success': False,
                'error': str(e)
            }
    
    def run_experiments(self):
        """Run experiments on all datasets using the specified framework"""
        print("\n" + "="*60)
        print("Starting In-Degree Distribution Experiments")
        print("="*60)
        print(f"Framework: {self.framework}")
        print(f"Datasets: {', '.join([d['name'] for d in self.datasets])}")
        print(f"Output directory: {self.output_dir}")
        print("")
        
        for dataset in self.datasets:
            dataset_name = dataset['name']
            input_path = dataset['path']
            
            print(f"\n\n{'#'*60}")
            print(f"# Dataset: {dataset_name}")
            print(f"# Path: {input_path}")
            print(f"# Framework: {self.framework}")
            print(f"{'#'*60}")
            
            # Run the specified framework experiment
            if self.framework == 'hadoop':
                metrics = self.run_hadoop_mapreduce(dataset_name, input_path)
            else:  # spark
                metrics = self.run_spark(dataset_name, input_path)
            
            # Update results for this framework and dataset
            self.update_results_for_framework(metrics)
            
            # Save intermediate results
            self.save_results()
            
            time.sleep(2)
        
        # Final summary
        self.print_summary()
        
        return self.results
    
    def save_results(self):
        """Save experiment results to JSON file"""
        with open(self.results_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\nResults saved to: {self.results_file}")
    
    def print_summary(self):
        """Print summary of all experiments"""
        print("\n\n" + "="*60)
        print("EXPERIMENT SUMMARY")
        print("="*60)
        
        # Group by framework
        hadoop_results = [r for r in self.results['experiments'] if r['framework'] == 'Hadoop MapReduce']
        spark_results = [r for r in self.results['experiments'] if r['framework'] == 'Apache Spark']
        
        if hadoop_results:
            print("\nHadoop MapReduce Results:")
            print("-" * 60)
            for result in hadoop_results:
                status = "✓" if result.get('success', False) else "✗"
                time_str = f"{result.get('execution_time', 0):.2f}s"
                print(f"  {status} {result['dataset']:20s} {time_str:>10s}")
        
        if spark_results:
            print("\nApache Spark Results:")
            print("-" * 60)
            for result in spark_results:
                status = "✓" if result.get('success', False) else "✗"
                time_str = f"{result.get('execution_time', 0):.2f}s"
                print(f"  {status} {result['dataset']:20s} {time_str:>10s}")
        
        # Performance comparison (only if both frameworks have results)
        if hadoop_results and spark_results:
            print("\n\nPerformance Comparison:")
            print("-" * 60)
            print(f"{'Dataset':<20s} {'Hadoop (s)':>12s} {'Spark (s)':>12s} {'Speedup':>10s}")
            print("-" * 60)
            
            for dataset in self.datasets:
                name = dataset['name']
                hadoop = next((r for r in hadoop_results if r['dataset'] == name), None)
                spark = next((r for r in spark_results if r['dataset'] == name), None)
                
                if hadoop and spark and hadoop.get('success') and spark.get('success'):
                    h_time = hadoop['execution_time']
                    s_time = spark['execution_time']
                    speedup = h_time / s_time if s_time > 0 else 0
                    print(f"{name:<20s} {h_time:>12.2f} {s_time:>12.2f} {speedup:>10.2f}x")
                else:
                    print(f"{name:<20s} {'N/A':>12s} {'N/A':>12s} {'N/A':>10s}")
        
        print("="*60)


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Run in-degree distribution experiments on multiple datasets'
    )
    parser.add_argument(
        '--framework',
        choices=['spark', 'hadoop'],
        required=True,
        help='Framework to run (spark or hadoop)'
    )
    parser.add_argument(
        '--datasets',
        nargs='+',
        choices=['email-EuAll', 'cit-Patents', 'soc-Pokec', 'soc-LiveJournal1', 'all'],
        default=['all'],
        help='Datasets to process (default: all)'
    )
    parser.add_argument(
        '--output-dir',
        default='results',
        help='Output directory for results (default: results)'
    )
    
    args = parser.parse_args()
    
    # Define dataset configurations
    all_datasets = [
        {
            'name': 'email-EuAll',
            'path': 'hdfs://hadoop:9000/user/root/snap_datasets/email-euall/email-euall.txt'
        },
        {
            'name': 'cit-Patents',
            'path': 'hdfs://hadoop:9000/user/root/snap_datasets/cit-patents/cit-patents.txt'
        },
        {
            'name': 'soc-Pokec',
            'path': 'hdfs://hadoop:9000/user/root/snap_datasets/soc-pokec-relationships/soc-pokec-relationships.txt'
        },
        {
            'name': 'soc-LiveJournal1',
            'path': 'hdfs://hadoop:9000/user/root/snap_datasets/soc-livejournal1/soc-livejournal1.txt'
        }
    ]
    
    # Select datasets
    if 'all' in args.datasets:
        datasets = all_datasets
    else:
        datasets = [d for d in all_datasets if d['name'] in args.datasets]
    
    # Run experiments
    runner = ExperimentRunner(datasets, args.output_dir, args.framework)
    results = runner.run_experiments()
    
    print(f"\n[OK] {args.framework.title()} experiments completed!")
    print(f"Results saved to: {args.output_dir}/experiment_results.json")


if __name__ == '__main__':
    main()
