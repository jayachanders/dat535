#!/usr/bin/env python3
"""
Advanced Spark Performance Pipeline for DAT535
Demonstrates performance optimization, window functions, and production patterns
"""

import os
import sys
import logging
import time
from datetime import datetime, timedelta
import random
from typing import Dict, Any, List

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    when, lit, desc, lag, row_number, rank, datediff, current_date,
    unix_timestamp, to_timestamp, to_date, hour, current_timestamp,
    countDistinct, expr, broadcast
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark import StorageLevel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PerformancePipelineConfig:
    """Configuration for the performance optimization pipeline"""
    
    def __init__(self):
        self.app_name = "DAT535-Performance-Pipeline"
        self.data_dir = os.path.expanduser("~/performance-pipeline-data")
        
        # Output directories
        self.bronze_dir = os.path.join(self.data_dir, "bronze")
        self.silver_dir = os.path.join(self.data_dir, "silver")
        self.gold_dir = os.path.join(self.data_dir, "gold")
        self.benchmark_dir = os.path.join(self.data_dir, "benchmarks")
        
        # Data generation settings
        self.num_sessions = 10000
        self.num_users = 2000
        self.days_of_data = 30
        
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create necessary directories"""
        for directory in [self.bronze_dir, self.silver_dir, self.gold_dir, self.benchmark_dir]:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"Ensured directory exists: {directory}")


class SessionDataGenerator:
    """Generate realistic user session data for performance testing"""
    
    @staticmethod
    def generate_sessions(num_sessions: int, num_users: int, days: int) -> List[Dict]:
        """Generate user session events"""
        logger.info(f"Generating {num_sessions} sessions for {num_users} users over {days} days...")
        
        sessions = []
        event_types = ['login', 'page_view', 'search', 'add_to_cart', 'purchase', 'logout']
        devices = ['mobile', 'desktop', 'tablet']
        regions = ['US-West', 'US-East', 'EU', 'APAC', 'LATAM']
        
        base_date = datetime.now() - timedelta(days=days)
        
        for i in range(num_sessions):
            user_id = random.randint(1, num_users)
            session_id = f"sess_{i:07d}"
            session_date = base_date + timedelta(
                days=random.randint(0, days-1),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            device = random.choice(devices)
            region = random.choice(regions)
            
            # Generate events for this session (1-10 events)
            num_events = random.randint(1, 10)
            for j in range(num_events):
                event_time = session_date + timedelta(minutes=j*2)
                event_type = random.choice(event_types)
                
                session = {
                    'session_id': session_id,
                    'user_id': user_id,
                    'timestamp': event_time.isoformat(),
                    'event': event_type,
                    'device': device,
                    'region': region,
                    'amount': round(random.uniform(10, 500), 2) if event_type == 'purchase' else 0.0,
                    'page_views': random.randint(1, 20) if event_type == 'page_view' else 0
                }
                sessions.append(session)
        
        logger.info(f"Generated {len(sessions)} total events")
        return sessions


class PerformancePipeline:
    """Advanced Spark performance optimization pipeline"""
    
    def __init__(self, config: PerformancePipelineConfig):
        self.config = config
        self.spark = self._create_spark_session()
        self.performance_metrics = {}
    
    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session"""
        logger.info("Creating Spark session with performance optimizations...")
        
        spark = SparkSession.builder \
            .appName(self.config.app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "20") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Application ID: {spark.sparkContext.applicationId}")
        logger.info(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        
        return spark
    
    def _benchmark(self, operation_name: str, func):
        """Benchmark a function and store metrics"""
        logger.info(f"Benchmarking: {operation_name}")
        start_time = time.time()
        result = func()
        duration = time.time() - start_time
        
        self.performance_metrics[operation_name] = {
            'duration': duration,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"✓ {operation_name} completed in {duration:.4f} seconds")
        return result
    
    def run_bronze_layer(self) -> Dict[str, Any]:
        """Bronze Layer: Raw data ingestion"""
        logger.info("=" * 60)
        logger.info("BRONZE LAYER: Raw Session Data Ingestion")
        logger.info("=" * 60)
        
        # Generate data
        generator = SessionDataGenerator()
        raw_sessions = generator.generate_sessions(
            self.config.num_sessions,
            self.config.num_users,
            self.config.days_of_data
        )
        
        # Create DataFrame
        def create_bronze():
            schema = StructType([
                StructField("session_id", StringType(), False),
                StructField("user_id", IntegerType(), False),
                StructField("timestamp", StringType(), True),
                StructField("event", StringType(), True),
                StructField("device", StringType(), True),
                StructField("region", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("page_views", IntegerType(), True)
            ])
            
            df = self.spark.createDataFrame(raw_sessions, schema)
            
            # Add ingestion metadata
            df = df.withColumn("_ingestion_timestamp", current_timestamp())
            
            # Save to Bronze layer
            bronze_path = os.path.join(self.config.bronze_dir, "sessions")
            df.write.mode("overwrite").parquet(bronze_path)
            
            return df.count()
        
        record_count = self._benchmark("bronze_ingestion", create_bronze)
        
        bronze_path = os.path.join(self.config.bronze_dir, "sessions")
        logger.info(f"✓ Saved {record_count:,} records to: {bronze_path}")
        
        return {
            'bronze_path': bronze_path,
            'record_count': record_count
        }
    
    def run_silver_layer(self, bronze_results: Dict[str, Any]) -> Dict[str, Any]:
        """Silver Layer: Advanced transformations with window functions"""
        logger.info("=" * 60)
        logger.info("SILVER LAYER: Advanced Transformations & Window Functions")
        logger.info("=" * 60)
        
        # Load Bronze data
        bronze_df = self.spark.read.parquet(bronze_results['bronze_path'])
        
        def apply_silver_transformations():
            # Type conversions and enrichment
            df = bronze_df \
                .withColumn("timestamp", to_timestamp(col("timestamp"))) \
                .withColumn("event_date", to_date(col("timestamp"))) \
                .withColumn("event_hour", hour(col("timestamp")))
            
            # Window Functions: User activity analysis
            logger.info("Applying window functions for session analysis...")
            
            user_window = Window.partitionBy("user_id").orderBy("timestamp")
            session_window = Window.partitionBy("session_id").orderBy("timestamp")
            
            df = df \
                .withColumn("prev_event_time", lag("timestamp").over(user_window)) \
                .withColumn("time_between_events_sec",
                    unix_timestamp("timestamp") - unix_timestamp("prev_event_time")) \
                .withColumn("running_total_spent",
                    spark_sum("amount").over(
                        Window.partitionBy("user_id").orderBy("timestamp")
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                    )) \
                .withColumn("session_event_number",
                    row_number().over(session_window)) \
                .withColumn("user_event_rank_by_amount",
                    rank().over(Window.partitionBy("user_id").orderBy(desc("amount"))))
            
            # Calculate moving averages
            moving_window = Window.partitionBy("user_id").orderBy("timestamp").rowsBetween(-2, 0)
            df = df.withColumn("moving_avg_amount", avg("amount").over(moving_window))
            
            # User segmentation
            df = df.withColumn("user_segment",
                when(col("running_total_spent") > 1000, "Premium")
                .when(col("running_total_spent") > 500, "Gold")
                .when(col("running_total_spent") > 100, "Silver")
                .otherwise("Bronze"))
            
            # Save to Silver layer
            silver_path = os.path.join(self.config.silver_dir, "enriched_sessions")
            df.write.mode("overwrite").parquet(silver_path)
            
            return silver_path, df.count()
        
        silver_path, silver_count = self._benchmark("silver_transformations", apply_silver_transformations)
        
        logger.info(f"✓ Saved {silver_count:,} enriched records to: {silver_path}")
        
        return {
            'silver_path': silver_path,
            'silver_count': silver_count
        }
    
    def run_gold_layer(self, silver_results: Dict[str, Any]) -> Dict[str, Any]:
        """Gold Layer: Business analytics with performance optimization"""
        logger.info("=" * 60)
        logger.info("GOLD LAYER: Performance-Optimized Analytics")
        logger.info("=" * 60)
        
        # Load Silver data
        silver_df = self.spark.read.parquet(silver_results['silver_path'])
        
        # Cache for multiple aggregations
        logger.info("Caching Silver data for multiple operations...")
        silver_df.cache()
        silver_df.count()  # Trigger caching
        
        # 1. User Behavior Analytics
        def create_user_analytics():
            logger.info("Creating user behavior analytics...")
            
            user_analytics = silver_df.groupBy("user_id", "device", "region").agg(
                count("*").alias("total_events"),
                countDistinct("session_id").alias("total_sessions"),
                countDistinct("event").alias("unique_event_types"),
                countDistinct("event_date").alias("active_days"),
                spark_sum("amount").alias("total_spent"),
                avg("amount").alias("avg_transaction"),
                spark_max("running_total_spent").alias("lifetime_value"),
                spark_max(col("user_segment")).alias("current_segment")
            ).withColumn("events_per_session",
                col("total_events") / col("total_sessions")
            ).withColumn("avg_daily_events",
                col("total_events") / col("active_days")
            )
            
            path = os.path.join(self.config.gold_dir, "user_analytics")
            user_analytics.write.mode("overwrite").parquet(path)
            return user_analytics.count()
        
        user_count = self._benchmark("user_analytics", create_user_analytics)
        logger.info(f"✓ User analytics: {user_count:,} records")
        
        # 2. Session Analytics with Broadcast Join
        def create_session_analytics():
            logger.info("Creating session analytics with broadcast optimization...")
            
            # Create small dimension table
            device_dim = self.spark.createDataFrame([
                ("mobile", "Mobile Device", "iOS/Android", "Small Screen"),
                ("desktop", "Desktop Computer", "Windows/Mac/Linux", "Large Screen"),
                ("tablet", "Tablet Device", "iOS/Android", "Medium Screen")
            ], ["device", "device_name", "os_family", "screen_size"])
            
            # Broadcast join for optimal performance
            session_summary = silver_df.groupBy("session_id", "device", "region").agg(
                count("*").alias("events_in_session"),
                spark_sum("amount").alias("session_revenue"),
                spark_min("timestamp").alias("session_start"),
                spark_max("timestamp").alias("session_end"),
                countDistinct("event").alias("unique_events")
            ).withColumn("session_duration_minutes",
                (unix_timestamp("session_end") - unix_timestamp("session_start")) / 60
            )
            
            # Apply broadcast join
            enriched_sessions = session_summary.join(
                broadcast(device_dim),
                on="device",
                how="left"
            )
            
            path = os.path.join(self.config.gold_dir, "session_analytics")
            enriched_sessions.write.mode("overwrite").parquet(path)
            return enriched_sessions.count()
        
        session_count = self._benchmark("session_analytics_with_broadcast", create_session_analytics)
        logger.info(f"✓ Session analytics: {session_count:,} records")
        
        # 3. Temporal Analytics (Partitioned Output)
        def create_temporal_analytics():
            logger.info("Creating temporal analytics with date partitioning...")
            
            daily_metrics = silver_df.groupBy("event_date", "region", "device").agg(
                count("*").alias("total_events"),
                countDistinct("user_id").alias("unique_users"),
                countDistinct("session_id").alias("unique_sessions"),
                spark_sum("amount").alias("daily_revenue"),
                avg("amount").alias("avg_transaction"),
                spark_sum(when(col("event") == "purchase", 1).otherwise(0)).alias("purchases")
            ).withColumn("conversion_rate",
                col("purchases") / col("unique_sessions") * 100
            )
            
            # Write with date partitioning for efficient queries
            path = os.path.join(self.config.gold_dir, "daily_metrics")
            daily_metrics.write.mode("overwrite").partitionBy("event_date").parquet(path)
            return daily_metrics.count()
        
        temporal_count = self._benchmark("temporal_analytics_partitioned", create_temporal_analytics)
        logger.info(f"✓ Temporal analytics: {temporal_count:,} records")
        
        # 4. Cohort Analysis
        def create_cohort_analysis():
            logger.info("Creating cohort analysis...")
            
            # User first activity date
            user_cohort = silver_df.groupBy("user_id").agg(
                spark_min("event_date").alias("cohort_date"),
                spark_max("event_date").alias("last_activity_date"),
                spark_sum("amount").alias("total_spent")
            )
            
            # Join back to calculate cohort metrics
            cohort_metrics = silver_df.join(user_cohort, on="user_id") \
                .withColumn("days_since_first_activity",
                    datediff(col("event_date"), col("cohort_date"))) \
                .groupBy("cohort_date", "days_since_first_activity").agg(
                    countDistinct("user_id").alias("active_users"),
                    spark_sum("amount").alias("cohort_revenue")
                )
            
            path = os.path.join(self.config.gold_dir, "cohort_analysis")
            cohort_metrics.write.mode("overwrite").parquet(path)
            return cohort_metrics.count()
        
        cohort_count = self._benchmark("cohort_analysis", create_cohort_analysis)
        logger.info(f"✓ Cohort analysis: {cohort_count:,} records")
        
        # Display sample analytics
        logger.info("\n--- User Analytics Sample ---")
        user_analytics_df = self.spark.read.parquet(os.path.join(self.config.gold_dir, "user_analytics"))
        user_analytics_df.orderBy(desc("lifetime_value")).show(10, truncate=False)
        
        # Unpersist cache
        silver_df.unpersist()
        
        return {
            'user_analytics_records': user_count,
            'session_analytics_records': session_count,
            'temporal_analytics_records': temporal_count,
            'cohort_analysis_records': cohort_count
        }
    
    def run_performance_benchmarks(self):
        """Run performance comparison benchmarks"""
        logger.info("=" * 60)
        logger.info("PERFORMANCE BENCHMARKS")
        logger.info("=" * 60)
        
        silver_path = os.path.join(self.config.silver_dir, "enriched_sessions")
        df = self.spark.read.parquet(silver_path)
        
        # Benchmark 1: Caching strategies
        logger.info("\n--- Benchmark: Caching Strategies ---")
        
        # No cache
        def query_no_cache():
            result = df.groupBy("user_id").agg(count("*"), spark_sum("amount")).count()
            return result
        
        no_cache_count = self._benchmark("query_no_cache", query_no_cache)
        
        # With cache
        cached_df = df.cache()
        cached_df.count()  # Trigger caching
        
        def query_with_cache():
            result = cached_df.groupBy("user_id").agg(count("*"), spark_sum("amount")).count()
            return result
        
        with_cache_count = self._benchmark("query_with_cache", query_with_cache)
        
        cached_df.unpersist()
        
        # Calculate improvement
        if self.performance_metrics["query_no_cache"]["duration"] > 0:
            improvement = ((self.performance_metrics["query_no_cache"]["duration"] - 
                          self.performance_metrics["query_with_cache"]["duration"]) / 
                          self.performance_metrics["query_no_cache"]["duration"] * 100)
            logger.info(f"Cache improvement: {improvement:.1f}%")
        
        # Benchmark 2: Filter optimization
        logger.info("\n--- Benchmark: Filter Optimization ---")
        
        def sequential_filters():
            result = df.filter(col("event") == "purchase") \
                      .filter(col("amount") > 100) \
                      .filter(col("device") == "mobile") \
                      .count()
            return result
        
        seq_count = self._benchmark("sequential_filters", sequential_filters)
        
        def combined_filters():
            result = df.filter(
                (col("event") == "purchase") & 
                (col("amount") > 100) & 
                (col("device") == "mobile")
            ).count()
            return result
        
        comb_count = self._benchmark("combined_filters", combined_filters)
        
        logger.info(f"Sequential: {seq_count}, Combined: {comb_count}")
    
    def save_performance_report(self):
        """Save performance metrics report"""
        logger.info("\n--- Performance Report ---")
        
        report_data = []
        for operation, metrics in self.performance_metrics.items():
            report_data.append({
                'operation': operation,
                'duration_seconds': metrics['duration'],
                'timestamp': metrics['timestamp']
            })
        
        report_df = self.spark.createDataFrame(report_data)
        report_path = os.path.join(self.config.benchmark_dir, "performance_metrics")
        report_df.write.mode("overwrite").parquet(report_path)
        
        logger.info("Performance metrics by operation:")
        report_df.orderBy(desc("duration_seconds")).show(truncate=False)
        
        logger.info(f"✓ Performance report saved to: {report_path}")
    
    def run(self):
        """Execute the complete pipeline"""
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("STARTING PERFORMANCE OPTIMIZATION PIPELINE")
        logger.info(f"Start Time: {start_time}")
        logger.info("=" * 60)
        
        try:
            # Run Bronze layer
            bronze_results = self.run_bronze_layer()
            
            # Run Silver layer
            silver_results = self.run_silver_layer(bronze_results)
            
            # Run Gold layer
            gold_results = self.run_gold_layer(silver_results)
            
            # Run performance benchmarks
            self.run_performance_benchmarks()
            
            # Save performance report
            self.save_performance_report()
            
            # Pipeline summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
            logger.info(f"End Time: {end_time}")
            logger.info(f"Total Duration: {duration:.2f} seconds")
            logger.info("=" * 60)
            
            logger.info("\n--- Pipeline Summary ---")
            logger.info(f"Bronze Layer: {bronze_results['record_count']:,} records")
            logger.info(f"Silver Layer: {silver_results['silver_count']:,} enriched records")
            logger.info(f"\nGold Layer:")
            logger.info(f"  User Analytics: {gold_results['user_analytics_records']:,} records")
            logger.info(f"  Session Analytics: {gold_results['session_analytics_records']:,} records")
            logger.info(f"  Temporal Analytics: {gold_results['temporal_analytics_records']:,} records")
            logger.info(f"  Cohort Analysis: {gold_results['cohort_analysis_records']:,} records")
            
            logger.info(f"\nData Location: {self.config.data_dir}")
            logger.info(f"Performance Report: {self.config.benchmark_dir}")
            
            return 0
            
        except Exception as e:
            logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
            return 1
        
        finally:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main entry point"""
    logger.info("Initializing Performance Optimization Pipeline...")
    
    # Create configuration
    config = PerformancePipelineConfig()
    
    # Create and run pipeline
    pipeline = PerformancePipeline(config)
    exit_code = pipeline.run()
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
