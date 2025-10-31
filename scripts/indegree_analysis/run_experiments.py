#!/usr/bin/env python3
"""
Experiment Runner for In-Degree Distribution Analysis
Runs both Hadoop and Spark implementations on multiple datasets and collects metrics
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
    
    def __init__(self, datasets, output_dir="results"):
        """
        Initialize experiment runner
        
        Args:
            datasets: List of dataset names to process
            output_dir: Directory to save results
        """
        self.datasets = datasets
        self.output_dir = output_dir
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'experiments': []
        }
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
    
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
        
        output_path = f"{self.output_dir}/hadoop_{dataset_name}_distribution"
        indegree_path = f"{self.output_dir}/hadoop_{dataset_name}_indegree"
        
        start_time = time.time()
        
        try:
            # Run MapReduce job for distribution
            cmd = [
                'python3',
                '/scripts/indegree_analysis/hadoop_indegree.py',
                '-r', 'hadoop',
                '--hadoop-streaming-jar', '/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar',
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
                'dataset': dataset_name,
                'input_path': input_path,
                'output_path': output_path,
                'execution_time': execution_time,
                'success': success,
                'stdout': result.stdout[-2000:] if result.stdout else '',  # Last 2000 chars
                'stderr': result.stderr[-2000:] if result.stderr else ''
            }
            
            if success:
                print(f"✓ Hadoop job completed successfully in {execution_time:.2f} seconds")
            else:
                print(f"✗ Hadoop job failed")
                print(f"Error: {result.stderr[:500]}")
            
            return metrics
            
        except subprocess.TimeoutExpired:
            print(f"✗ Hadoop job timed out after 30 minutes")
            return {
                'framework': 'Hadoop MapReduce',
                'dataset': dataset_name,
                'execution_time': 1800,
                'success': False,
                'error': 'Timeout'
            }
        except Exception as e:
            print(f"✗ Error running Hadoop job: {str(e)}")
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
        
        output_path = f"{self.output_dir}/spark_{dataset_name}_distribution"
        
        start_time = time.time()
        
        try:
            # Run Spark job
            cmd = [
                'spark-submit',
                '--master', 'local[*]',
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
                except:
                    pass
            
            if success:
                print(f"✓ Spark job completed successfully in {execution_time:.2f} seconds")
            else:
                print(f"✗ Spark job failed")
                print(f"Error: {result.stderr[:500]}")
            
            return metrics
            
        except subprocess.TimeoutExpired:
            print(f"✗ Spark job timed out after 30 minutes")
            return {
                'framework': 'Apache Spark',
                'dataset': dataset_name,
                'execution_time': 1800,
                'success': False,
                'error': 'Timeout'
            }
        except Exception as e:
            print(f"✗ Error running Spark job: {str(e)}")
            return {
                'framework': 'Apache Spark',
                'dataset': dataset_name,
                'success': False,
                'error': str(e)
            }
    
    def run_experiments(self):
        """Run all experiments on all datasets"""
        print("\n" + "="*60)
        print("Starting In-Degree Distribution Experiments")
        print("="*60)
        print(f"Datasets: {', '.join([d['name'] for d in self.datasets])}")
        print(f"Output directory: {self.output_dir}")
        print("")
        
        for dataset in self.datasets:
            dataset_name = dataset['name']
            input_path = dataset['path']
            
            print(f"\n\n{'#'*60}")
            print(f"# Dataset: {dataset_name}")
            print(f"# Path: {input_path}")
            print(f"{'#'*60}")
            
            # Run Hadoop experiment
            hadoop_metrics = self.run_hadoop_mapreduce(dataset_name, input_path)
            self.results['experiments'].append(hadoop_metrics)
            
            # Small delay between experiments
            time.sleep(2)
            
            # Run Spark experiment
            spark_metrics = self.run_spark(dataset_name, input_path)
            self.results['experiments'].append(spark_metrics)
            
            # Save intermediate results
            self.save_results()
            
            time.sleep(2)
        
        # Final summary
        self.print_summary()
        
        return self.results
    
    def save_results(self):
        """Save experiment results to JSON file"""
        results_file = os.path.join(self.output_dir, 'experiment_results.json')
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        print(f"\nResults saved to: {results_file}")
    
    def print_summary(self):
        """Print summary of all experiments"""
        print("\n\n" + "="*60)
        print("EXPERIMENT SUMMARY")
        print("="*60)
        
        # Group by framework
        hadoop_results = [r for r in self.results['experiments'] if r['framework'] == 'Hadoop MapReduce']
        spark_results = [r for r in self.results['experiments'] if r['framework'] == 'Apache Spark']
        
        print("\nHadoop MapReduce Results:")
        print("-" * 60)
        for result in hadoop_results:
            status = "✓" if result.get('success', False) else "✗"
            time_str = f"{result.get('execution_time', 0):.2f}s"
            print(f"  {status} {result['dataset']:20s} {time_str:>10s}")
        
        print("\nApache Spark Results:")
        print("-" * 60)
        for result in spark_results:
            status = "✓" if result.get('success', False) else "✗"
            time_str = f"{result.get('execution_time', 0):.2f}s"
            print(f"  {status} {result['dataset']:20s} {time_str:>10s}")
        
        # Performance comparison
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
            'path': '/user/root/snap_datasets/email-EuAll/email-EuAll.txt'
        },
        {
            'name': 'cit-Patents',
            'path': '/user/root/snap_datasets/cit-Patents/cit-Patents.txt'
        },
        {
            'name': 'soc-Pokec',
            'path': '/user/root/snap_datasets/soc-Pokec/soc-pokec-relationships.txt'
        },
        {
            'name': 'soc-LiveJournal1',
            'path': '/user/root/snap_datasets/soc-LiveJournal1/soc-LiveJournal1.txt'
        }
    ]
    
    # Select datasets
    if 'all' in args.datasets:
        datasets = all_datasets
    else:
        datasets = [d for d in all_datasets if d['name'] in args.datasets]
    
    # Run experiments
    runner = ExperimentRunner(datasets, args.output_dir)
    results = runner.run_experiments()
    
    print("\n✓ All experiments completed!")
    print(f"Results saved to: {args.output_dir}/experiment_results.json")


if __name__ == '__main__':
    main()
