#!/usr/bin/env python3
"""
MapReduce Pipeline for DAT535 - Demonstrating MapReduce Patterns in Spark
Implements Bronze → Silver → Gold architecture with MapReduce operations
"""

import os
import json
import logging
import time
from datetime import datetime, timedelta
import random
from typing import List, Dict, Any

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    round as spark_round, when, to_timestamp, to_date, hour, current_timestamp,
    unix_timestamp, countDistinct
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MapReducePipelineConfig:
    """Configuration for the MapReduce pipeline"""
    
    def __init__(self):
        self.app_name = "DAT535-MapReduce-Pipeline"
        self.data_dir = os.path.expanduser("~/mapreduce-pipeline-data")
        
        # Medallion Architecture directories
        self.bronze_dir = os.path.join(self.data_dir, "bronze")
        self.silver_dir = os.path.join(self.data_dir, "silver")
        self.gold_dir = os.path.join(self.data_dir, "gold")
        
        # Data generation settings
        self.num_events = 100000
        self.num_users = 5000
        self.num_products = 500
        
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create necessary directories"""
        for directory in [self.bronze_dir, self.silver_dir, self.gold_dir]:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"Ensured directory exists: {directory}")


class EventDataGenerator:
    """Generate synthetic event stream data"""
    
    @staticmethod
    def generate_events(num_events: int, num_users: int, num_products: int) -> List[str]:
        """Generate event logs as JSON strings (simulating raw API data)"""
        logger.info(f"Generating {num_events} event logs...")
        
        events = []
        event_types = ['login', 'view', 'purchase', 'logout', 'signup', 'search', 'add_to_cart']
        devices = ['mobile', 'desktop', 'tablet']
        locations = ['US', 'CA', 'UK', 'DE', 'FR', 'JP', 'AU', 'IN', 'BR', 'MX']
        pages = ['homepage', 'product', 'checkout', 'account', 'help', 'about']
        
        start_time = datetime.now() - timedelta(days=7)
        
        for i in range(num_events):
            timestamp = start_time + timedelta(seconds=random.randint(0, 604800))  # Within 7 days
            event_type = random.choice(event_types)
            
            event = {
                'timestamp': timestamp.isoformat(),
                'user_id': random.randint(1, num_users),
                'event': event_type,
                'device': random.choice(devices),
                'location': random.choice(locations)
            }
            
            # Add event-specific fields
            if event_type == 'purchase':
                event['product_id'] = f'PROD_{random.randint(1, num_products):04d}'
                event['amount'] = round(random.uniform(10, 500), 2)
                event['quantity'] = random.randint(1, 5)
            elif event_type == 'view':
                event['page'] = random.choice(pages)
                event['duration_seconds'] = random.randint(5, 300)
            elif event_type == 'search':
                event['query'] = random.choice(['laptop', 'phone', 'book', 'shoes', 'watch'])
            elif event_type == 'signup':
                event['email'] = f'user{event["user_id"]}@example.com'
            
            # Occasionally generate malformed data (5% of the time)
            if random.random() < 0.05:
                if random.random() < 0.5:
                    event['user_id'] = 'invalid'  # Invalid type
                else:
                    event = {'malformed': 'data', 'timestamp': timestamp.isoformat()}
            
            events.append(json.dumps(event))
        
        logger.info(f"Generated {num_events} events")
        return events


class MapReducePipeline:
    """Main MapReduce pipeline implementing Medallion Architecture"""
    
    def __init__(self, config: MapReducePipelineConfig):
        self.config = config
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        logger.info("Creating Spark session...")
        
        spark = SparkSession.builder \
            .appName(self.config.app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "20") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Application ID: {spark.sparkContext.applicationId}")
        
        return spark
    
    def run_bronze_layer(self) -> Dict[str, Any]:
        """Bronze Layer: Raw data ingestion with MapReduce pattern"""
        logger.info("=" * 60)
        logger.info("BRONZE LAYER: Raw Event Stream Ingestion")
        logger.info("=" * 60)
        
        # Generate raw event data
        generator = EventDataGenerator()
        raw_events = generator.generate_events(
            self.config.num_events,
            self.config.num_users,
            self.config.num_products
        )
        
        # Create RDD from raw JSON strings (MapReduce pattern)
        logger.info("Processing raw events using MapReduce pattern...")
        raw_rdd = self.spark.sparkContext.parallelize(raw_events)
        
        # Map phase: Parse JSON and add metadata
        def parse_event_with_metadata(json_str: str) -> Dict[str, Any]:
            """Map function: Parse JSON and enrich with metadata"""
            try:
                event = json.loads(json_str)
                event['_ingestion_timestamp'] = datetime.now().isoformat()
                event['_source'] = 'event_api'
                event['_status'] = 'valid'
                event['_bronze_layer'] = True
                # Ensure all optional fields have default values
                event['amount'] = event.get('amount', None)
                event['quantity'] = event.get('quantity', None)
                event['duration_seconds'] = event.get('duration_seconds', None)
                event['product_id'] = event.get('product_id', None)
                event['query'] = event.get('query', None)
                event['page'] = event.get('page', None)
                event['email'] = event.get('email', None)
                return event
            except Exception as e:
                return {
                    '_raw_data': json_str,
                    '_ingestion_timestamp': datetime.now().isoformat(),
                    '_source': 'event_api',
                    '_status': 'parse_error',
                    '_error_message': str(e),
                    '_bronze_layer': True,
                    'amount': None,
                    'quantity': None,
                    'duration_seconds': None,
                    'product_id': None,
                    'query': None,
                    'page': None,
                    'email': None
                }
        
        # Apply map transformation
        bronze_rdd = raw_rdd.map(parse_event_with_metadata)
        
        # Convert to DataFrame for storage
        bronze_df = self.spark.createDataFrame(bronze_rdd)
        
        # Save to Bronze layer
        bronze_path = os.path.join(self.config.bronze_dir, "raw_events")
        bronze_df.write.mode("overwrite").parquet(bronze_path)
        logger.info(f"✓ Saved raw events to: {bronze_path}")
        
        # Data quality metrics
        total_records = bronze_df.count()
        valid_records = bronze_df.filter(col("_status") == "valid").count()
        error_records = bronze_df.filter(col("_status") == "parse_error").count()
        
        logger.info("\nBronze Layer Quality Metrics:")
        logger.info(f"  Total records: {total_records:,}")
        logger.info(f"  Valid records: {valid_records:,}")
        logger.info(f"  Parse errors: {error_records:,}")
        logger.info(f"  Success rate: {(valid_records/total_records)*100:.2f}%")
        
        return {
            'bronze_path': bronze_path,
            'total_records': total_records,
            'valid_records': valid_records,
            'error_records': error_records
        }
    
    def run_silver_layer(self, bronze_results: Dict[str, Any]) -> Dict[str, Any]:
        """Silver Layer: Data cleaning and enrichment with MapReduce patterns"""
        logger.info("=" * 60)
        logger.info("SILVER LAYER: Data Cleaning and Enrichment")
        logger.info("=" * 60)
        
        # Load Bronze data
        bronze_df = self.spark.read.parquet(bronze_results['bronze_path'])
        
        # Filter valid records only
        valid_df = bronze_df.filter(col("_status") == "valid")
        
        # MapReduce Pattern 1: Event Type Aggregation
        logger.info("Applying MapReduce pattern: Event type aggregation...")
        
        # Map phase: Extract event type
        event_rdd = valid_df.rdd.map(lambda row: (row['event'], 1))
        
        # Reduce phase: Count by event type
        event_counts = event_rdd.reduceByKey(lambda a, b: a + b).collect()
        
        logger.info("\nEvent Type Distribution (MapReduce):")
        for event_type, event_count in sorted(event_counts, key=lambda x: x[1], reverse=True):
            logger.info(f"  {event_type}: {count:,}")
        
        # Silver layer transformations
        logger.info("\nApplying Silver layer transformations...")
        
        silver_df = valid_df \
            .withColumn("timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("user_id", col("user_id").cast("integer")) \
            .withColumn("event_date", to_date(col("timestamp"))) \
            .withColumn("event_hour", hour(col("timestamp")))
        
        # Cast optional numeric columns if they exist (handle None values)
        if "amount" in valid_df.columns:
            silver_df = silver_df.withColumn("amount", col("amount").cast("double"))
        
        if "quantity" in valid_df.columns:
            silver_df = silver_df.withColumn("quantity", col("quantity").cast("integer"))
        
        if "duration_seconds" in valid_df.columns:
            silver_df = silver_df.withColumn("duration_seconds", col("duration_seconds").cast("integer"))
        
        silver_df = silver_df \
            .filter(col("user_id").isNotNull()) \
            .withColumn("_silver_processed_timestamp", current_timestamp()) \
            .drop("_ingestion_timestamp", "_source", "_status", "_bronze_layer")
        
        # Save to Silver layer
        silver_path = os.path.join(self.config.silver_dir, "cleaned_events")
        silver_df.write.mode("overwrite").parquet(silver_path)
        logger.info(f"✓ Saved cleaned events to: {silver_path}")
        
        # MapReduce Pattern 2: User-Event Aggregation
        logger.info("\nApplying MapReduce pattern: User activity aggregation...")
        
        # Create (user_id, event) pairs and count
        user_event_rdd = silver_df.rdd.map(lambda row: ((row['user_id'], row['event']), 1))
        user_event_counts = user_event_rdd.reduceByKey(lambda a, b: a + b)
        
        # Find most active users
        user_activity_rdd = user_event_rdd.map(lambda x: (x[0][0], 1)) \
                                          .reduceByKey(lambda a, b: a + b) \
                                          .takeOrdered(10, key=lambda x: -x[1])
        
        logger.info("\nTop 10 Most Active Users (MapReduce):")
        for user_id, event_count in user_activity_rdd:
            logger.info(f"  User {user_id}: {event_count} events")
        
        silver_count = silver_df.count()
        
        return {
            'silver_path': silver_path,
            'silver_count': silver_count,
            'event_counts': event_counts,
            'top_users': user_activity_rdd
        }
    
    def run_gold_layer(self, silver_results: Dict[str, Any]) -> Dict[str, Any]:
        """Gold Layer: Business analytics with advanced MapReduce aggregations"""
        logger.info("=" * 60)
        logger.info("GOLD LAYER: Business Analytics")
        logger.info("=" * 60)
        
        # Load Silver data
        silver_df = self.spark.read.parquet(silver_results['silver_path'])
        
        # Cache for multiple operations
        silver_df.cache()
        
        # 1. User Engagement Metrics
        logger.info("Calculating user engagement metrics...")
        
        user_engagement = silver_df.groupBy("user_id").agg(
            count("*").alias("total_events"),
            countDistinct("event").alias("unique_event_types"),
            countDistinct("event_date").alias("active_days"),
            spark_sum("amount").alias("total_spent"),
            spark_sum("quantity").alias("total_items_purchased"),
            avg("duration_seconds").alias("avg_session_duration"),
            spark_min("timestamp").alias("first_seen"),
            spark_max("timestamp").alias("last_seen")
        )\
        .withColumn(
            "days_active_period",
            (unix_timestamp("last_seen") - unix_timestamp("first_seen")) / 86400
        )\
        .withColumn(
            "engagement_score",
            spark_round((col("total_events") * col("active_days")) / (col("days_active_period") + 1), 2)
        )
        
        # Save user engagement
        user_engagement_path = os.path.join(self.config.gold_dir, "user_engagement")
        user_engagement.write.mode("overwrite").parquet(user_engagement_path)
        logger.info("✓ Saved user engagement metrics")
        
        # 2. Event Funnel Analysis
        logger.info("Calculating event funnel metrics...")
        
        funnel_metrics = silver_df.groupBy("event_date").agg(
            count("*").alias("total_events"),
            countDistinct("user_id").alias("unique_users"),
            spark_sum(when(col("event") == "view", 1).otherwise(0)).alias("views"),
            spark_sum(when(col("event") == "add_to_cart", 1).otherwise(0)).alias("add_to_carts"),
            spark_sum(when(col("event") == "purchase", 1).otherwise(0)).alias("purchases"),
            spark_sum("amount").alias("daily_revenue")
        ).withColumn("view_to_cart_rate",
            spark_round(col("add_to_carts") / col("views") * 100, 2)
        ).withColumn("cart_to_purchase_rate",
            spark_round(col("purchases") / col("add_to_carts") * 100, 2)
        ).withColumn("overall_conversion_rate",
            spark_round(col("purchases") / col("views") * 100, 2)
        )
        
        funnel_path = os.path.join(self.config.gold_dir, "event_funnel")
        funnel_metrics.write.mode("overwrite").parquet(funnel_path)
        logger.info("✓ Saved funnel analysis")
        
        # 3. Device & Location Analytics (MapReduce pattern)
        logger.info("Calculating device and location analytics using MapReduce...")
        
        # MapReduce: Device-Location combinations
        device_location_rdd = silver_df.rdd.map(
            lambda row: ((row['device'], row['location']), 1)
        ).reduceByKey(lambda a, b: a + b)
        
        # Convert to DataFrame
        device_location_data = device_location_rdd.map(
            lambda x: (x[0][0], x[0][1], x[1])
        ).collect()
        
        device_location_df = self.spark.createDataFrame(
            device_location_data,
            ['device', 'location', 'event_count']  # Added missing closing bracket
        )
        
        # Save device & location analytics
        device_location_path = os.path.join(self.config.gold_dir, "device_location_analytics")
        device_location_df.write.mode("overwrite").parquet(device_location_path)
        logger.info("✓ Saved device & location analytics")
        
        # 4. Revenue Analysis
        logger.info("Calculating revenue analysis...")
        
        revenue_analysis = silver_df.groupBy("event_date").agg(
            spark_sum("amount").alias("total_revenue"),
            countDistinct("user_id").alias("unique_customers"),
            count(when(col("event") == "purchase", True)).alias("total_purchases"),
            avg("amount").alias("avg_order_value")
        )
        
        revenue_path = os.path.join(self.config.gold_dir, "revenue_analysis")
        revenue_analysis.write.mode("overwrite").parquet(revenue_path)
        logger.info("✓ Saved revenue analysis")
        
        # Unpersist cached Silver DataFrame
        silver_df.unpersist()
        
        return {
            'gold_dir': self.config.gold_dir,
            'user_engagement_path': user_engagement_path,
            'funnel_path': funnel_path,
            'device_location_path': device_location_path,
            'revenue_path': revenue_path
        }


def run_pipeline():
    """Run the complete MapReduce pipeline: Bronze → Silver → Gold"""
    config = MapReducePipelineConfig()
    pipeline = MapReducePipeline(config)
    
    # Bronze layer
    bronze_results = pipeline.run_bronze_layer()
    logger.info(f"Bronze layer results: {bronze_results}")
    
    # Silver layer
    silver_results = pipeline.run_silver_layer(bronze_results)
    logger.info(f"Silver layer results: {silver_results}")
    
    # Gold layer
    gold_results = pipeline.run_gold_layer(silver_results)
    logger.info(f"Gold layer results: {gold_results}")
    
    return gold_results

def main() -> int:
    """Main entry point returning status code for callers"""
    start_time = time.time()
    try:
        run_pipeline()
        logger.info(f"Total execution time: {time.time() - start_time:.2f} seconds")
        logger.info("MapReduce Pipeline execution completed successfully.")
        return 0
    except Exception as e:
        logger.error(f"MapReduce Pipeline execution failed: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    main()
