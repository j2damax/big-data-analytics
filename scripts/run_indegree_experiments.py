#!/usr/bin/env python3
"""
Experiment Runner: In-Degree Distribution Analysis
Runs both Hadoop MapReduce and Spark implementations on multiple datasets
Collects performance metrics and verifies correctness
"""

import subprocess
import time
import json
import os
from datetime import datetime
from pathlib import Path


class InDegreeExperimentRunner:
    """
    Orchestrates experiments comparing Hadoop MapReduce and Spark
    """
    
    def __init__(self):
        self.results_dir = Path('/tmp/indegree_results')
        self.results_dir.mkdir(exist_ok=True)
        self.experiments = []
        
        # Dataset configurations
        self.datasets = {
            'email-EuAll': {
                'name': 'email-EuAll',
                'hdfs_path': 'hdfs://hadoop:9000/user/root/snap_datasets/email-EuAll/email-EuAll.txt',
                'size': 'Small (~420K edges)'
            },
            'cit-Patents': {
                'name': 'cit-Patents',
                'hdfs_path': 'hdfs://hadoop:9000/user/root/snap_datasets/cit-Patents/cit-Patents.txt',
                'size': 'Medium (~16.5M edges)'
            },
            'soc-Pokec': {
                'name': 'soc-Pokec',
                'hdfs_path': 'hdfs://hadoop:9000/user/root/snap_datasets/soc-Pokec/soc-pokec-relationships.txt',
                'size': 'Large (~30.6M edges)'
            }
        }
    
    def run_mapreduce_experiment(self, dataset_name, hdfs_path):
        """
        Run Hadoop MapReduce implementation
        
        Args:
            dataset_name: Name of the dataset
            hdfs_path: HDFS path to input file
        
        Returns:
            Dictionary with results and metrics
        """
        print(f"\n{'='*70}")
        print(f"Running MapReduce on {dataset_name}")
        print(f"{'='*70}")
        
        output_dir = self.results_dir / f'mapreduce_{dataset_name}_{int(time.time())}'
        
        start_time = time.time()
        
        try:
            # Run MapReduce job using mrjob with Hadoop runner
            cmd = [
                'python3', '/scripts/indegree_mapreduce.py',
                '-r', 'hadoop',
                '--hadoop-bin', 'hadoop',
                '--hadoop-streaming-jar', '/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar',
                '--output-dir', str(output_dir),
                hdfs_path
            ]
            
            print(f"Command: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                print(f"✓ MapReduce job completed successfully")
                print(f"  Execution time: {execution_time:.2f} seconds")
                
                # Read results
                results = self._read_mapreduce_output(output_dir)
                
                return {
                    'success': True,
                    'execution_time': execution_time,
                    'output_dir': str(output_dir),
                    'results': results,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
            else:
                print(f"✗ MapReduce job failed")
                print(f"  Error: {result.stderr}")
                return {
                    'success': False,
                    'execution_time': execution_time,
                    'error': result.stderr
                }
                
        except subprocess.TimeoutExpired:
            print(f"✗ MapReduce job timed out after 1 hour")
            return {
                'success': False,
                'error': 'Timeout after 1 hour'
            }
        except Exception as e:
            print(f"✗ MapReduce job failed with exception: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def run_spark_experiment(self, dataset_name, hdfs_path):
        """
        Run Spark implementation
        
        Args:
            dataset_name: Name of the dataset
            hdfs_path: HDFS path to input file
        
        Returns:
            Dictionary with results and metrics
        """
        print(f"\n{'='*70}")
        print(f"Running Spark on {dataset_name}")
        print(f"{'='*70}")
        
        output_dir = self.results_dir / f'spark_{dataset_name}_{int(time.time())}'
        
        start_time = time.time()
        
        try:
            # Run Spark job using spark-submit
            cmd = [
                'spark-submit',
                '--master', 'spark://spark-master:7077',
                '--deploy-mode', 'client',
                '/scripts/indegree_spark.py',
                hdfs_path,
                str(output_dir)
            ]
            
            print(f"Command: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                print(f"✓ Spark job completed successfully")
                print(f"  Execution time: {execution_time:.2f} seconds")
                
                # Parse execution time from output
                results = self._read_spark_output(output_dir)
                
                return {
                    'success': True,
                    'execution_time': execution_time,
                    'output_dir': str(output_dir),
                    'results': results,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
            else:
                print(f"✗ Spark job failed")
                print(f"  Error: {result.stderr}")
                return {
                    'success': False,
                    'execution_time': execution_time,
                    'error': result.stderr
                }
                
        except subprocess.TimeoutExpired:
            print(f"✗ Spark job timed out after 1 hour")
            return {
                'success': False,
                'error': 'Timeout after 1 hour'
            }
        except Exception as e:
            print(f"✗ Spark job failed with exception: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _read_mapreduce_output(self, output_dir):
        """Read and parse MapReduce output"""
        results = {}
        output_file = output_dir / 'part-00000'
        
        if output_file.exists():
            with open(output_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        # Parse "indegree\tcount" format
                        parts = line.split('\t')
                        if len(parts) == 2:
                            try:
                                # Handle quoted keys and strip whitespace consistently
                                indegree_str = parts[0].strip().strip('"')
                                count_str = parts[1].strip()
                                indegree = int(indegree_str)
                                count = int(count_str)
                                results[indegree] = count
                            except (ValueError, IndexError) as e:
                                # Skip lines that don't parse correctly
                                print(f"Warning: Could not parse line: {line} - {e}")
        
        return results
    
    def _read_spark_output(self, output_dir):
        """Read and parse Spark output"""
        results = {}
        
        # Spark creates part files in the output directory
        if output_dir.exists():
            for part_file in output_dir.glob('part-*'):
                with open(part_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            # Parse "(indegree, count)" format
                            line = line.strip('()')
                            parts = line.split(',')
                            if len(parts) == 2:
                                indegree = int(parts[0].strip())
                                count = int(parts[1].strip())
                                results[indegree] = count
        
        return results
    
    def verify_correctness(self, mapreduce_results, spark_results, dataset_name):
        """
        Verify that both implementations produce identical results
        
        Args:
            mapreduce_results: Results from MapReduce
            spark_results: Results from Spark
            dataset_name: Name of dataset being compared
        
        Returns:
            Boolean indicating if results match
        """
        print(f"\n{'='*70}")
        print(f"Verifying Correctness: {dataset_name}")
        print(f"{'='*70}")
        
        if mapreduce_results == spark_results:
            print("✓ Results are IDENTICAL - Correctness verified!")
            print(f"  MapReduce: {len(mapreduce_results)} unique in-degrees")
            print(f"  Spark:     {len(spark_results)} unique in-degrees")
            return True
        else:
            print("✗ Results DIFFER - Investigation needed")
            print(f"  MapReduce: {len(mapreduce_results)} unique in-degrees")
            print(f"  Spark:     {len(spark_results)} unique in-degrees")
            
            # Find differences
            all_keys = set(mapreduce_results.keys()) | set(spark_results.keys())
            differences = []
            for key in all_keys:
                mr_val = mapreduce_results.get(key, 0)
                spark_val = spark_results.get(key, 0)
                if mr_val != spark_val:
                    differences.append((key, mr_val, spark_val))
            
            print(f"  Differences found: {len(differences)}")
            if differences:
                print("  First 5 differences:")
                for i, (indegree, mr_count, spark_count) in enumerate(differences[:5]):
                    print(f"    In-degree {indegree}: MR={mr_count}, Spark={spark_count}")
            
            return False
    
    def run_all_experiments(self, datasets_to_test=None):
        """
        Run experiments on all or selected datasets
        
        Args:
            datasets_to_test: List of dataset names to test, or None for all
        """
        print(f"\n{'#'*70}")
        print(f"# In-Degree Distribution Experiment Suite")
        print(f"# Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'#'*70}\n")
        
        if datasets_to_test is None:
            datasets_to_test = list(self.datasets.keys())
        
        all_results = {}
        
        for dataset_name in datasets_to_test:
            if dataset_name not in self.datasets:
                print(f"Warning: Unknown dataset '{dataset_name}', skipping...")
                continue
            
            dataset_info = self.datasets[dataset_name]
            print(f"\n{'*'*70}")
            print(f"* Dataset: {dataset_name}")
            print(f"* Size: {dataset_info['size']}")
            print(f"{'*'*70}")
            
            # Run MapReduce
            mr_result = self.run_mapreduce_experiment(
                dataset_name, 
                dataset_info['hdfs_path']
            )
            
            # Run Spark
            spark_result = self.run_spark_experiment(
                dataset_name,
                dataset_info['hdfs_path']
            )
            
            # Verify correctness
            if mr_result['success'] and spark_result['success']:
                correctness = self.verify_correctness(
                    mr_result['results'],
                    spark_result['results'],
                    dataset_name
                )
            else:
                correctness = False
                print(f"\n✗ Cannot verify correctness - one or both jobs failed")
            
            # Store results
            all_results[dataset_name] = {
                'dataset_info': dataset_info,
                'mapreduce': mr_result,
                'spark': spark_result,
                'correctness_verified': correctness,
                'timestamp': datetime.now().isoformat()
            }
        
        # Save comprehensive results
        results_file = self.results_dir / f'experiment_results_{int(time.time())}.json'
        with open(results_file, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        
        print(f"\n{'#'*70}")
        print(f"# Experiment Suite Complete")
        print(f"# Results saved to: {results_file}")
        print(f"{'#'*70}\n")
        
        # Print summary
        self._print_summary(all_results)
        
        return all_results
    
    def _print_summary(self, all_results):
        """Print summary of all experiments"""
        print("\n" + "="*70)
        print("EXPERIMENT SUMMARY")
        print("="*70)
        
        for dataset_name, result in all_results.items():
            print(f"\n{dataset_name}:")
            print(f"  Size: {result['dataset_info']['size']}")
            
            if result['mapreduce']['success']:
                print(f"  MapReduce: ✓ {result['mapreduce']['execution_time']:.2f}s")
            else:
                print(f"  MapReduce: ✗ Failed")
            
            if result['spark']['success']:
                print(f"  Spark:     ✓ {result['spark']['execution_time']:.2f}s")
            else:
                print(f"  Spark:     ✗ Failed")
            
            if result['correctness_verified']:
                print(f"  Correctness: ✓ Verified")
            else:
                print(f"  Correctness: ✗ Not verified")
            
            # Calculate speedup if both succeeded
            if result['mapreduce']['success'] and result['spark']['success']:
                mr_time = result['mapreduce']['execution_time']
                spark_time = result['spark']['execution_time']
                speedup = mr_time / spark_time if spark_time > 0 else 0
                print(f"  Speedup: {speedup:.2f}x (Spark vs MapReduce)")


def main():
    """Main execution"""
    import sys
    
    runner = InDegreeExperimentRunner()
    
    # Check if specific datasets requested
    if len(sys.argv) > 1:
        datasets = sys.argv[1:]
        print(f"Running experiments on selected datasets: {datasets}")
    else:
        datasets = None
        print("Running experiments on all datasets")
    
    runner.run_all_experiments(datasets)


if __name__ == '__main__':
    main()
