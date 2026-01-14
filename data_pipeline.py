#!/usr/bin/env python3
"""
Data Pipeline for DAT535 - Spark Cluster Execution
Converts the Lab 2 exploratory notebook into a production pipeline
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
import random
from typing import Dict, Any

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    round as spark_round, when, lit, concat, desc, to_date
)
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataPipelineConfig:
    """Configuration for the data pipeline"""
    
    def __init__(self):
        self.app_name = "DAT535-Data-Pipeline"
        self.data_dir = os.path.expanduser("~/pipeline-data")
        
        # Create data directories following Medallion Architecture
        self.bronze_dir = os.path.join(self.data_dir, "bronze")
        self.silver_dir = os.path.join(self.data_dir, "silver")
        self.gold_dir = os.path.join(self.data_dir, "gold")
        
        # Sample data configuration
        self.num_customers = 10000
        self.num_transactions = 50000
        self.num_products = 200
        
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Create necessary directories"""
        for directory in [self.bronze_dir, self.silver_dir, self.gold_dir]:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"Ensured directory exists: {directory}")


class DataGenerator:
    """Generate sample data for the pipeline"""
    
    @staticmethod
    def generate_customers(spark: SparkSession, num_customers: int):
        """Generate customer data"""
        logger.info(f"Generating {num_customers} customers...")
        
        customers_data = []
        first_names = ['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Diana', 'Eve', 'Frank', 
                      'Grace', 'Henry', 'Ivy', 'Jack', 'Kelly', 'Leo', 'Mia', 'Noah']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 
                     'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Wilson', 'Anderson']
        cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 
                 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'Austin']
        
        for i in range(1, num_customers + 1):
            customer = {
                'customer_id': i,
                'first_name': random.choice(first_names),
                'last_name': random.choice(last_names),
                'email': f'customer{i}@email.com',
                'age': random.randint(18, 80),
                'city': random.choice(cities),
                'signup_date': (datetime.now() - timedelta(days=random.randint(1, 730))).strftime('%Y-%m-%d'),
                'total_orders': random.randint(0, 100),
                'total_spent': round(random.uniform(0, 10000), 2)
            }
            customers_data.append(customer)
        
        schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("signup_date", StringType(), True),
            StructField("total_orders", IntegerType(), True),
            StructField("total_spent", DoubleType(), True)
        ])
        
        return spark.createDataFrame(customers_data, schema)
    
    @staticmethod
    def generate_transactions(spark: SparkSession, num_transactions: int, num_customers: int):
        """Generate transaction data"""
        logger.info(f"Generating {num_transactions} transactions...")
        
        transactions_data = []
        categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Food', 'Toys', 'Health']
        payment_methods = ['credit_card', 'debit_card', 'paypal', 'cash', 'apple_pay', 'google_pay']
        statuses = ['completed', 'pending', 'cancelled', 'refunded']
        devices = ['mobile', 'desktop', 'tablet']
        browsers = ['chrome', 'firefox', 'safari', 'edge']
        locations = ['US', 'CA', 'UK', 'DE', 'FR', 'JP', 'AU']
        
        for i in range(1, num_transactions + 1):
            transaction = {
                'transaction_id': f'TXN_{i:07d}',
                'customer_id': random.randint(1, num_customers),
                'product_id': f'PROD_{random.randint(1, 200):04d}',
                'product_category': random.choice(categories),
                'amount': round(random.uniform(5, 1000), 2),
                'timestamp': (datetime.now() - timedelta(hours=random.randint(1, 8760))).isoformat(),
                'payment_method': random.choice(payment_methods),
                'status': random.choice(statuses),
                'device': random.choice(devices),
                'browser': random.choice(browsers),
                'location': random.choice(locations)
            }
            transactions_data.append(transaction)
        
        return spark.createDataFrame(transactions_data)
    
    @staticmethod
    def generate_products(spark: SparkSession, num_products: int):
        """Generate product data"""
        logger.info(f"Generating {num_products} products...")
        
        products_data = []
        categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Food', 'Toys', 'Health']
        suppliers = ['Supplier A', 'Supplier B', 'Supplier C', 'Supplier D', 'Supplier E']
        
        for i in range(1, num_products + 1):
            product = {
                'product_id': f'PROD_{i:04d}',
                'product_name': f'Product {i}',
                'category': random.choice(categories),
                'price': round(random.uniform(10, 2000), 2),
                'stock_quantity': random.randint(0, 2000),
                'supplier': random.choice(suppliers),
                'weight_kg': round(random.uniform(0.1, 100), 2),
                'created_date': (datetime.now() - timedelta(days=random.randint(30, 1825))).strftime('%Y-%m-%d'),
                'is_active': random.choice([True, False])
            }
            products_data.append(product)
        
        return spark.createDataFrame(products_data)


class DataPipeline:
    """Main data pipeline orchestrator"""
    
    def __init__(self, config: DataPipelineConfig):
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
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Application ID: {spark.sparkContext.applicationId}")
        
        return spark
    
    def run_bronze_layer(self) -> Dict[str, Any]:
        """Bronze Layer: Raw data ingestion"""
        logger.info("=" * 60)
        logger.info("BRONZE LAYER: Raw Data Ingestion")
        logger.info("=" * 60)
        
        generator = DataGenerator()
        
        # Generate and save raw data
        customers_df = generator.generate_customers(self.spark, self.config.num_customers)
        transactions_df = generator.generate_transactions(
            self.spark, self.config.num_transactions, self.config.num_customers
        )
        products_df = generator.generate_products(self.spark, self.config.num_products)
        
        # Save to Bronze layer (raw format - Parquet)
        customers_path = os.path.join(self.config.bronze_dir, "customers")
        transactions_path = os.path.join(self.config.bronze_dir, "transactions")
        products_path = os.path.join(self.config.bronze_dir, "products")
        
        customers_df.write.mode("overwrite").parquet(customers_path)
        logger.info(f"✓ Saved customers to: {customers_path}")
        
        transactions_df.write.mode("overwrite").parquet(transactions_path)
        logger.info(f"✓ Saved transactions to: {transactions_path}")
        
        products_df.write.mode("overwrite").parquet(products_path)
        logger.info(f"✓ Saved products to: {products_path}")
        
        return {
            'customers_path': customers_path,
            'transactions_path': transactions_path,
            'products_path': products_path,
            'customers_count': customers_df.count(),
            'transactions_count': transactions_df.count(),
            'products_count': products_df.count()
        }
    
    def run_silver_layer(self, bronze_paths: Dict[str, Any]) -> Dict[str, Any]:
        """Silver Layer: Data cleansing and enrichment"""
        logger.info("=" * 60)
        logger.info("SILVER LAYER: Data Cleansing and Enrichment")
        logger.info("=" * 60)
        
        # Load from Bronze
        customers_df = self.spark.read.parquet(bronze_paths['customers_path'])
        transactions_df = self.spark.read.parquet(bronze_paths['transactions_path'])
        products_df = self.spark.read.parquet(bronze_paths['products_path'])
        
        # Data Quality Checks
        logger.info("Running data quality checks...")
        
        # Check null values
        null_counts = customers_df.select([
            count(when(col(c).isNull(), c)).alias(f"{c}_nulls") 
            for c in customers_df.columns
        ]).collect()[0].asDict()
        
        total_nulls = sum(null_counts.values())
        logger.info(f"Total null values in customers: {total_nulls}")
        
        # Enrich customer data
        enriched_customers = customers_df \
            .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name"))) \
            .withColumn("avg_order_value", 
                       when(col("total_orders") > 0, 
                            spark_round(col("total_spent") / col("total_orders"), 2))
                       .otherwise(0.0)) \
            .withColumn("customer_segment", 
                       when(col("total_spent") > 3000, "Premium")
                       .when(col("total_spent") > 1500, "Gold")
                       .when(col("total_spent") > 500, "Silver")
                       .otherwise("Bronze")) \
            .withColumn("signup_date", to_date(col("signup_date")))
        
        # Clean transactions - filter completed only
        clean_transactions = transactions_df \
            .filter(col("status") == "completed") \
            .filter(col("amount") > 0) \
            .withColumn("transaction_date", to_date(col("timestamp")))
        
        # Active products only
        active_products = products_df \
            .filter(col("is_active") == True) \
            .filter(col("stock_quantity") > 0) \
            .withColumn("created_date", to_date(col("created_date")))
        
        # Save to Silver layer
        silver_customers_path = os.path.join(self.config.silver_dir, "customers_enriched")
        silver_transactions_path = os.path.join(self.config.silver_dir, "transactions_clean")
        silver_products_path = os.path.join(self.config.silver_dir, "products_active")
        
        enriched_customers.write.mode("overwrite").parquet(silver_customers_path)
        logger.info(f"✓ Saved enriched customers to: {silver_customers_path}")
        
        clean_transactions.write.mode("overwrite").parquet(silver_transactions_path)
        logger.info(f"✓ Saved clean transactions to: {silver_transactions_path}")
        
        active_products.write.mode("overwrite").parquet(silver_products_path)
        logger.info(f"✓ Saved active products to: {silver_products_path}")
        
        return {
            'customers_path': silver_customers_path,
            'transactions_path': silver_transactions_path,
            'products_path': silver_products_path,
            'customers_count': enriched_customers.count(),
            'transactions_count': clean_transactions.count(),
            'products_count': active_products.count()
        }
    
    def run_gold_layer(self, silver_paths: Dict[str, Any]) -> Dict[str, Any]:
        """Gold Layer: Business-level aggregations"""
        logger.info("=" * 60)
        logger.info("GOLD LAYER: Business Aggregations")
        logger.info("=" * 60)
        
        # Load from Silver
        customers_df = self.spark.read.parquet(silver_paths['customers_path'])
        transactions_df = self.spark.read.parquet(silver_paths['transactions_path'])
        products_df = self.spark.read.parquet(silver_paths['products_path'])
        
        # 1. Customer Analytics
        customer_analytics = customers_df.groupBy("customer_segment", "city").agg(
            count("*").alias("customer_count"),
            spark_round(avg("age"), 1).alias("avg_age"),
            spark_round(avg("total_spent"), 2).alias("avg_total_spent"),
            spark_round(avg("avg_order_value"), 2).alias("avg_order_value"),
            spark_sum("total_spent").alias("segment_revenue")
        ).orderBy(desc("segment_revenue"))
        
        # 2. Transaction Analytics by Category
        category_analytics = transactions_df.groupBy("product_category").agg(
            count("*").alias("transaction_count"),
            spark_round(spark_sum("amount"), 2).alias("total_revenue"),
            spark_round(avg("amount"), 2).alias("avg_transaction_value"),
            spark_min("amount").alias("min_transaction"),
            spark_max("amount").alias("max_transaction")
        ).orderBy(desc("total_revenue"))
        
        # 3. Payment Method Analytics
        payment_analytics = transactions_df.groupBy("payment_method").agg(
            count("*").alias("usage_count"),
            spark_round(spark_sum("amount"), 2).alias("total_amount"),
            spark_round(avg("amount"), 2).alias("avg_amount")
        ).orderBy(desc("usage_count"))
        
        # 4. Device Analytics
        device_analytics = transactions_df.groupBy("device", "browser").agg(
            count("*").alias("transaction_count"),
            spark_round(spark_sum("amount"), 2).alias("total_revenue")
        ).orderBy(desc("transaction_count"))
        
        # 5. Product Analytics
        product_analytics = products_df.groupBy("category", "supplier").agg(
            count("*").alias("product_count"),
            spark_round(avg("price"), 2).alias("avg_price"),
            spark_sum("stock_quantity").alias("total_stock")
        ).orderBy("category", desc("product_count"))
        
        # Save to Gold layer
        gold_base = self.config.gold_dir
        
        customer_analytics.write.mode("overwrite").parquet(os.path.join(gold_base, "customer_analytics"))
        logger.info(f"✓ Saved customer analytics")
        
        category_analytics.write.mode("overwrite").parquet(os.path.join(gold_base, "category_analytics"))
        logger.info(f"✓ Saved category analytics")
        
        payment_analytics.write.mode("overwrite").parquet(os.path.join(gold_base, "payment_analytics"))
        logger.info(f"✓ Saved payment analytics")
        
        device_analytics.write.mode("overwrite").parquet(os.path.join(gold_base, "device_analytics"))
        logger.info(f"✓ Saved device analytics")
        
        product_analytics.write.mode("overwrite").parquet(os.path.join(gold_base, "product_analytics"))
        logger.info(f"✓ Saved product analytics")
        
        # Log sample results
        logger.info("\n--- Customer Segment Analysis ---")
        customer_analytics.show(10, truncate=False)
        
        logger.info("\n--- Top Product Categories ---")
        category_analytics.show(10, truncate=False)
        
        logger.info("\n--- Payment Methods Usage ---")
        payment_analytics.show(truncate=False)
        
        return {
            'customer_analytics_records': customer_analytics.count(),
            'category_analytics_records': category_analytics.count(),
            'payment_analytics_records': payment_analytics.count(),
            'device_analytics_records': device_analytics.count(),
            'product_analytics_records': product_analytics.count()
        }
    
    def run(self):
        """Execute the complete pipeline"""
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("STARTING DATA PIPELINE EXECUTION")
        logger.info(f"Start Time: {start_time}")
        logger.info("=" * 60)
        
        try:
            # Run Bronze layer
            bronze_results = self.run_bronze_layer()
            
            # Run Silver layer
            silver_results = self.run_silver_layer(bronze_results)
            
            # Run Gold layer
            gold_results = self.run_gold_layer(silver_results)
            
            # Pipeline summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
            logger.info(f"End Time: {end_time}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info("=" * 60)
            
            logger.info("\n--- Pipeline Summary ---")
            logger.info(f"Bronze Layer:")
            logger.info(f"  Customers: {bronze_results['customers_count']:,}")
            logger.info(f"  Transactions: {bronze_results['transactions_count']:,}")
            logger.info(f"  Products: {bronze_results['products_count']:,}")
            
            logger.info(f"\nSilver Layer:")
            logger.info(f"  Enriched Customers: {silver_results['customers_count']:,}")
            logger.info(f"  Clean Transactions: {silver_results['transactions_count']:,}")
            logger.info(f"  Active Products: {silver_results['products_count']:,}")
            
            logger.info(f"\nGold Layer:")
            logger.info(f"  Customer Analytics: {gold_results['customer_analytics_records']:,} records")
            logger.info(f"  Category Analytics: {gold_results['category_analytics_records']:,} records")
            logger.info(f"  Payment Analytics: {gold_results['payment_analytics_records']:,} records")
            logger.info(f"  Device Analytics: {gold_results['device_analytics_records']:,} records")
            logger.info(f"  Product Analytics: {gold_results['product_analytics_records']:,} records")
            
            logger.info(f"\nData Location: {self.config.data_dir}")
            
            return 0
            
        except Exception as e:
            logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
            return 1
        
        finally:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main entry point"""
    logger.info("Initializing Data Pipeline...")
    
    # Create configuration
    config = DataPipelineConfig()
    
    # Create and run pipeline
    pipeline = DataPipeline(config)
    exit_code = pipeline.run()
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
