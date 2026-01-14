#!/usr/bin/env python3
"""
Pipeline Runner for DAT535
Allows selection of which pipeline to execute
"""

import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description='DAT535 Spark Pipeline Runner')
    parser.add_argument('pipeline', choices=['data', 'mapreduce', 'performance', 'all'],
                       help='Which pipeline to run: data (basic), mapreduce (advanced), performance (optimization), or all')
    
    args = parser.parse_args()
    
    if args.pipeline == 'data' or args.pipeline == 'all':
        print("=" * 70)
        print("RUNNING DATA PIPELINE (Bronze → Silver → Gold)")
        print("=" * 70)
        from data_pipeline import main as data_main
        exit_code = data_main()
        if exit_code != 0:
            sys.exit(exit_code)
    
    if args.pipeline == 'mapreduce' or args.pipeline == 'all':
        print("\n" + "=" * 70)
        print("RUNNING MAPREDUCE PIPELINE (MapReduce Patterns)")
        print("=" * 70)
        from mapreduce_pipeline import main as mapreduce_main
        exit_code = mapreduce_main()
        if exit_code != 0:
            sys.exit(exit_code)
    
    if args.pipeline == 'performance' or args.pipeline == 'all':
        print("\n" + "=" * 70)
        print("RUNNING PERFORMANCE PIPELINE (Advanced Optimizations)")
        print("=" * 70)
        from performance_pipeline import main as performance_main
        exit_code = performance_main()
        if exit_code != 0:
            sys.exit(exit_code)
    
    print("\n" + "=" * 70)
    print("ALL PIPELINES COMPLETED SUCCESSFULLY")
    print("=" * 70)
    sys.exit(0)

if __name__ == "__main__":
    main()
