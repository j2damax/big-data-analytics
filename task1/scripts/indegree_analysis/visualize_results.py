#!/usr/bin/env python3
"""
Visualization script for in-degree distribution results
Generates plots and analysis for comparison between Hadoop and Spark
"""

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for server environments
import matplotlib.pyplot as plt
import json
import os
import sys
import argparse
from collections import defaultdict
import numpy as np


class ResultVisualizer:
    """Visualize and analyze in-degree distribution results"""
    
    def __init__(self, results_file, output_dir="plots"):
        """
        Initialize visualizer
        
        Args:
            results_file: Path to experiment results JSON file
            output_dir: Directory to save plots
        """
        self.results_file = results_file
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Load results
        with open(results_file, 'r') as f:
            self.results = json.load(f)
    
    def parse_distribution_file(self, file_path):
        """
        Parse distribution output file
        
        Args:
            file_path: Path to distribution file (Hadoop or Spark output)
            
        Returns:
            List of (degree, count) tuples
        """
        distribution = []
        
        # Check if it's a directory (Spark/Hadoop output)
        if os.path.isdir(file_path):
            # Read part files
            for filename in sorted(os.listdir(file_path)):
                if filename.startswith('part-'):
                    with open(os.path.join(file_path, filename), 'r') as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            # Parse different formats
                            if '\t' in line:
                                parts = line.split('\t')
                            else:
                                # Handle tuple format from Spark: (degree, count)
                                line = line.strip('()')
                                parts = line.split(',')
                            
                            if len(parts) >= 2:
                                try:
                                    degree = int(parts[0].strip())
                                    count = int(parts[1].strip())
                                    distribution.append((degree, count))
                                except ValueError:
                                    continue
        
        return sorted(distribution)
    
    def plot_distribution(self, distribution, title, output_file):
        """
        Create scatter plot of in-degree distribution
        
        Args:
            distribution: List of (degree, count) tuples
            title: Plot title
            output_file: Output file path
        """
        if not distribution:
            print(f"Warning: No distribution data for {title}")
            return
        
        degrees = [d[0] for d in distribution]
        counts = [d[1] for d in distribution]
        
        plt.figure(figsize=(10, 6))
        plt.scatter(degrees, counts, alpha=0.6, s=10)
        plt.xlabel('In-Degree')
        plt.ylabel('Number of Nodes')
        plt.title(title)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(output_file, dpi=150)
        plt.close()
        
        print(f"✓ Saved plot: {output_file}")
    
    def plot_loglog_distribution(self, distribution, title, output_file):
        """
        Create log-log plot of in-degree distribution
        
        Args:
            distribution: List of (degree, count) tuples
            title: Plot title
            output_file: Output file path
        """
        if not distribution:
            print(f"Warning: No distribution data for {title}")
            return
        
        # Filter out zeros for log plot
        filtered = [(d, c) for d, c in distribution if d > 0 and c > 0]
        
        if not filtered:
            print(f"Warning: No valid data for log-log plot of {title}")
            return
        
        degrees = [d[0] for d in filtered]
        counts = [d[1] for d in filtered]
        
        plt.figure(figsize=(10, 6))
        plt.scatter(degrees, counts, alpha=0.6, s=10)
        plt.xscale('log')
        plt.yscale('log')
        plt.xlabel('In-Degree (log scale)')
        plt.ylabel('Number of Nodes (log scale)')
        plt.title(title + ' (Log-Log Scale)')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(output_file, dpi=150)
        plt.close()
        
        print(f"✓ Saved log-log plot: {output_file}")
    
    def plot_performance_comparison(self):
        """Create performance comparison bar chart"""
        # Group results by dataset and framework
        datasets = set()
        hadoop_times = {}
        spark_times = {}
        
        for exp in self.results['experiments']:
            if not exp.get('success', False):
                continue
            
            dataset = exp['dataset']
            datasets.add(dataset)
            exec_time = exp['execution_time']
            
            if exp['framework'] == 'Hadoop MapReduce':
                hadoop_times[dataset] = exec_time
            elif exp['framework'] == 'Apache Spark':
                spark_times[dataset] = exec_time
        
        datasets = sorted(list(datasets))
        
        # Create grouped bar chart
        x = np.arange(len(datasets))
        width = 0.35
        
        hadoop_values = [hadoop_times.get(d, 0) for d in datasets]
        spark_values = [spark_times.get(d, 0) for d in datasets]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        bars1 = ax.bar(x - width/2, hadoop_values, width, label='Hadoop MapReduce', color='#FF6B6B')
        bars2 = ax.bar(x + width/2, spark_values, width, label='Apache Spark', color='#4ECDC4')
        
        ax.set_xlabel('Dataset')
        ax.set_ylabel('Execution Time (seconds)')
        ax.set_title('Performance Comparison: Hadoop vs Spark')
        ax.set_xticks(x)
        ax.set_xticklabels(datasets, rotation=15, ha='right')
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                           f'{height:.1f}s',
                           ha='center', va='bottom', fontsize=8)
        
        plt.tight_layout()
        output_file = os.path.join(self.output_dir, 'performance_comparison.png')
        plt.savefig(output_file, dpi=150)
        plt.close()
        
        print(f"✓ Saved performance comparison: {output_file}")
    
    def generate_distribution_plots(self):
        """Generate in-degree distribution plots from results JSON"""
        experiments = self.results.get('experiments', [])
        if not experiments:
            print("Warning: No experiments found in results JSON")
            return
        
        for exp in experiments:
            if not exp.get('success', False):
                continue
            dataset = exp.get('dataset', 'unknown')
            framework = exp.get('framework', 'unknown')
            dist_map = exp.get('degree_distribution', {})
            if not isinstance(dist_map, dict) or not dist_map:
                # Skip if no distribution embedded in results
                continue
            
            # Convert mapping {degree: count} to sorted list of tuples
            try:
                distribution = sorted(
                    [(int(k), int(v)) for k, v in dist_map.items()],
                    key=lambda x: x[0]
                )
            except Exception:
                # If keys are not numeric strings, attempt alternative parsing or skip
                continue
            
            # Build safe filenames
            safe_dataset = dataset.replace('/', '-').replace(' ', '-')
            safe_framework = framework.replace(' ', '-')
            base_name = f"{safe_dataset}__{safe_framework}"
            
            scatter_path = os.path.join(
                self.output_dir,
                f"indegree_scatter__{base_name}.png"
            )
            loglog_path = os.path.join(
                self.output_dir,
                f"indegree_loglog__{base_name}.png"
            )
            
            title = f"In-Degree Distribution: {dataset} ({framework})"
            self.plot_distribution(distribution, title, scatter_path)
            self.plot_loglog_distribution(distribution, title, loglog_path)
    
    def generate_report(self):
        """Generate comprehensive analysis report"""
        report_file = os.path.join(self.output_dir, 'ANALYSIS_REPORT.md')
        
        with open(report_file, 'w') as f:
            f.write("# In-Degree Distribution Analysis Report\n\n")
            f.write(f"**Generated:** {self.results['timestamp']}\n\n")
            
            f.write("## Experiment Results\n\n")
            
            # Results table
            f.write("### Execution Times\n\n")
            f.write("| Dataset | Hadoop MapReduce | Apache Spark | Speedup |\n")
            f.write("|---------|------------------|--------------|----------|\n")
            
            datasets = set()
            results_by_dataset = defaultdict(dict)
            
            for exp in self.results['experiments']:
                dataset = exp['dataset']
                datasets.add(dataset)
                framework = 'hadoop' if exp['framework'] == 'Hadoop MapReduce' else 'spark'
                results_by_dataset[dataset][framework] = exp
            
            for dataset in sorted(datasets):
                hadoop = results_by_dataset[dataset].get('hadoop', {})
                spark = results_by_dataset[dataset].get('spark', {})
                
                h_time = hadoop.get('execution_time', 0) if hadoop.get('success') else 'Failed'
                s_time = spark.get('execution_time', 0) if spark.get('success') else 'Failed'
                
                if isinstance(h_time, (int, float)) and isinstance(s_time, (int, float)) and s_time > 0:
                    speedup = f"{h_time / s_time:.2f}x"
                    h_time = f"{h_time:.2f}s"
                    s_time = f"{s_time:.2f}s"
                else:
                    speedup = "N/A"
                    if isinstance(h_time, (int, float)):
                        h_time = f"{h_time:.2f}s"
                    if isinstance(s_time, (int, float)):
                        s_time = f"{s_time:.2f}s"
                
                f.write(f"| {dataset} | {h_time} | {s_time} | {speedup} |\n")
            
            f.write("\n### Statistics\n\n")
            
            for exp in self.results['experiments']:
                if exp.get('success') and exp['framework'] == 'Apache Spark':
                    f.write(f"**{exp['dataset']}** (Spark):\n")
                    if 'total_nodes' in exp:
                        f.write(f"- Total nodes with in-degree > 0: {exp['total_nodes']}\n")
                    if 'max_indegree' in exp:
                        f.write(f"- Maximum in-degree: {exp['max_indegree']}\n")
                    if 'avg_indegree' in exp:
                        f.write(f"- Average in-degree: {exp['avg_indegree']:.2f}\n")
                    f.write("\n")
            
            f.write("## Analysis\n\n")
            
            f.write("### Performance Patterns\n\n")
            f.write("1. **Execution Speed**: Comparison shows the relative performance ")
            f.write("of Hadoop MapReduce vs Apache Spark for in-degree computation.\n\n")
            
            f.write("2. **Scalability**: The performance difference becomes more ")
            f.write("pronounced with larger datasets.\n\n")
            
            f.write("3. **In-Memory Processing**: Spark's in-memory processing provides ")
            f.write("significant advantages for iterative graph operations.\n\n")
            
            f.write("### System Comparison\n\n")
            f.write("**Hadoop MapReduce:**\n")
            f.write("- Disk-based processing with high I/O overhead\n")
            f.write("- Better for very large datasets that don't fit in memory\n")
            f.write("- More mature fault tolerance mechanisms\n\n")
            
            f.write("**Apache Spark:**\n")
            f.write("- In-memory processing for faster computation\n")
            f.write("- Lower latency for iterative operations\n")
            f.write("- More efficient for graph analytics\n")
            f.write("- Better suited for large-scale graph processing\n\n")
            
            f.write("## Visualizations\n\n")
            f.write("The following plots are generated:\n\n")
            f.write("1. **In-Degree Distribution Plots**: Scatter plots showing ")
            f.write("the distribution of in-degrees across nodes\n")
            f.write("2. **Log-Log Distribution Plots**: Useful for identifying ")
            f.write("power-law distributions common in social networks\n")
            f.write("3. **Performance Comparison**: Bar chart comparing execution ")
            f.write("times between Hadoop and Spark\n\n")
            
            f.write("## Conclusions\n\n")
            f.write("This analysis demonstrates the practical differences between ")
            f.write("Hadoop MapReduce and Apache Spark for graph analytics. ")
            f.write("The in-degree distribution computation serves as a fundamental ")
            f.write("graph operation that highlights the strengths and weaknesses ")
            f.write("of each framework.\n")
        
        print(f"✓ Generated report: {report_file}")
    
    def run(self):
        """Run all visualizations"""
        print("\n" + "="*60)
        print("Generating Visualizations and Analysis")
        print("="*60)
        
        # Create performance comparison
        self.plot_performance_comparison()
        
        # Generate in-degree distribution plots from embedded results
        self.generate_distribution_plots()
        
        # Generate report
        self.generate_report()
        
        print("\n✓ All visualizations complete!")
        print(f"Output directory: {self.output_dir}")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Visualize in-degree distribution experiment results'
    )
    parser.add_argument(
        '--results',
        default='results/experiment_results.json',
        help='Path to experiment results JSON file'
    )
    parser.add_argument(
        '--output-dir',
        default='plots',
        help='Output directory for plots'
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.results):
        print(f"Error: Results file not found: {args.results}")
        print("Please run experiments first using run_experiments.py")
        sys.exit(1)
    
    visualizer = ResultVisualizer(args.results, args.output_dir)
    visualizer.run()


if __name__ == '__main__':
    main()
