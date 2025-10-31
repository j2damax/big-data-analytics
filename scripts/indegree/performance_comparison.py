#!/usr/bin/env python3
"""
Performance Comparison Tool
Compare different in-degree distribution implementations with web monitoring integration
"""

import subprocess
import time
import sys
import json
import urllib.request
import urllib.error

def get_yarn_metrics():
    """Get YARN cluster metrics via REST API"""
    try:
        with urllib.request.urlopen('http://localhost:8088/ws/v1/cluster/metrics', timeout=5) as response:
            data = json.loads(response.read().decode())
            metrics = data['clusterMetrics']
            return {
                'allocated_memory': metrics.get('allocatedMB', 0),
                'available_memory': metrics.get('availableMB', 0),
                'allocated_cores': metrics.get('allocatedVirtualCores', 0),
                'available_cores': metrics.get('availableVirtualCores', 0),
                'active_nodes': metrics.get('activeNodes', 0)
            }
    except (urllib.error.URLError, json.JSONDecodeError, KeyError):
        return None

def get_hdfs_metrics():
    """Get HDFS metrics via JMX API"""
    try:
        url = 'http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState'
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read().decode())
            if data['beans']:
                bean = data['beans'][0]
                return {
                    'capacity_used': bean.get('CapacityUsed', 0),
                    'capacity_total': bean.get('CapacityTotal', 0),
                    'files_total': bean.get('FilesTotal', 0),
                    'blocks_total': bean.get('BlocksTotal', 0)
                }
    except (urllib.error.URLError, json.JSONDecodeError, KeyError, IndexError):
        return None

def print_metrics(label, yarn_metrics, hdfs_metrics):
    """Print system metrics in a formatted way"""
    print(f"\n📊 {label} Metrics:")
    print("=" * 40)
    
    if yarn_metrics:
        print(f"🔧 YARN Cluster:")
        print(f"   Memory: {yarn_metrics['allocated_memory']:,} MB allocated / {yarn_metrics['available_memory']:,} MB available")
        print(f"   Cores:  {yarn_metrics['allocated_cores']} allocated / {yarn_metrics['available_cores']} available")
        print(f"   Nodes:  {yarn_metrics['active_nodes']} active")
    else:
        print("🔧 YARN Cluster: Metrics unavailable")
    
    if hdfs_metrics:
        capacity_pct = (hdfs_metrics['capacity_used'] / hdfs_metrics['capacity_total'] * 100) if hdfs_metrics['capacity_total'] > 0 else 0
        print(f"💾 HDFS Storage:")
        print(f"   Capacity: {hdfs_metrics['capacity_used']:,} / {hdfs_metrics['capacity_total']:,} bytes ({capacity_pct:.1f}% used)")
        print(f"   Files:    {hdfs_metrics['files_total']:,}")
        print(f"   Blocks:   {hdfs_metrics['blocks_total']:,}")
    else:
        print("💾 HDFS Storage: Metrics unavailable")

def print_monitoring_info():
    """Print information about available monitoring interfaces"""
    print("\n🌐 Web Monitoring Interfaces:")
    print("=" * 40)
    print("📊 Hadoop HDFS:        http://localhost:9870")
    print("🔧 YARN ResourceMgr:   http://localhost:8088")
    print("⚡ Spark Master:       http://localhost:8080")
    print("🔄 Spark Worker:       http://localhost:8081")
    print("🌊 Flink Dashboard:    http://localhost:8082")
    print("\n💡 Tip: Open these URLs in your browser for real-time monitoring!")
    print("💡 Use 'make monitor-all' to open all interfaces automatically")

def run_simple(input_file):
    """Run optimized implementation"""
    start = time.time()
    try:
        result = subprocess.run(['python3', 'scripts/indegree/indegree_distribution.py', input_file], 
                              capture_output=True, text=True)
        return time.time() - start, result.returncode == 0, result.stdout
    except Exception as e:
        return time.time() - start, False, str(e)

def run_complex(input_file):
    """Run complex implementation"""
    start = time.time()
    try:
        result = subprocess.run(['python3', 'scripts/hadoop_indegree.py', input_file, 'test'], 
                              capture_output=True, text=True)
        return time.time() - start, result.returncode == 0, result.stdout
    except Exception as e:
        return time.time() - start, False, str(e)

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 scripts/indegree/performance_comparison.py <input_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    print("🎯 IN-DEGREE PERFORMANCE ANALYSIS WITH WEB MONITORING")
    print("=" * 60)
    
    # Show monitoring interfaces
    print_monitoring_info()
    
    # Get baseline metrics
    print("\n📈 Capturing baseline system metrics...")
    baseline_yarn = get_yarn_metrics()
    baseline_hdfs = get_hdfs_metrics()
    print_metrics("BASELINE", baseline_yarn, baseline_hdfs)
    
    print("\n" + "="*60)
    print("🚀 STARTING PERFORMANCE COMPARISON")
    print("="*60)
    
    # Run simple version
    print("🔄 Running optimized implementation...")
    print("   💡 Monitor progress at: http://localhost:8088 (YARN)")
    simple_time, simple_success, simple_output = run_simple(input_file)
    
    # Get metrics after simple run
    post_simple_yarn = get_yarn_metrics()
    post_simple_hdfs = get_hdfs_metrics()
    
    print("\n🔄 Running complex implementation (if available)...")
    complex_time, complex_success, complex_output = run_complex(input_file)
    
    # Get final metrics
    final_yarn = get_yarn_metrics()
    final_hdfs = get_hdfs_metrics()
    
    # Results
    print("⏱️  EXECUTION TIMES:")
    if simple_success:
        print(f"   ✅ Optimized: {simple_time:.2f}s ({len(open('scripts/indegree/indegree_distribution.py').readlines())} lines)")
    else:
        print("   ❌ Optimized: Failed")
    
    if complex_success:
        print(f"   ✅ Complex:   {complex_time:.2f}s (legacy implementation)")
    else:
        print("   ❌ Complex:   Failed (implementation not available)")
    
    if simple_success and complex_success:
        if simple_time < complex_time:
            speedup = complex_time / simple_time
            print(f"\n🏆 Winner: Optimized implementation ({speedup:.1f}x faster!)")
        else:
            speedup = simple_time / complex_time
            print(f"\n🤔 Winner: Complex implementation ({speedup:.1f}x faster)")
        
        overhead = abs(complex_time - simple_time) / min(simple_time, complex_time) * 100
        print(f"📈 Performance difference: {overhead:.1f}%")
    
    # Show system metrics progression
    if post_simple_yarn and baseline_yarn:
        print("\n📊 RESOURCE UTILIZATION:")
        memory_change = post_simple_yarn['allocated_memory'] - baseline_yarn['allocated_memory']
        cores_change = post_simple_yarn['allocated_cores'] - baseline_yarn['allocated_cores']
        print(f"   Memory change: {memory_change:+,} MB")
        print(f"   Cores change:  {cores_change:+}")
    
    print_metrics("FINAL", final_yarn, final_hdfs)
    
    print(f"\n🌐 For detailed monitoring, visit:")
    print(f"   • YARN Applications: http://localhost:8088/cluster/apps")
    print(f"   • System Metrics:    http://localhost:8088/cluster/metrics")
    print(f"   • HDFS Status:       http://localhost:9870/dfshealth.html")
    
    print(f"\n💡 Run 'make monitor-all' to open all monitoring interfaces!")

if __name__ == '__main__':
    main()