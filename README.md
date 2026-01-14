# DAT535 Data Pipeline

A production-ready Spark data pipeline implementing the Medallion Architecture (Bronze → Silver → Gold).

## Overview

This pipeline demonstrates enterprise data processing patterns using PySpark on a private OpenStack cluster with automated deployment via GitHub Actions.

## Architecture

### Medallion Architecture Layers

```
Bronze Layer (Raw Data)
    ├── customers.parquet      # Raw customer data
    ├── transactions.parquet   # Raw transaction logs  
    └── products.parquet       # Raw product catalog

Silver Layer (Cleaned & Enriched)
    ├── customers_enriched.parquet    # Enriched with segments, calculated fields
    ├── transactions_clean.parquet    # Filtered completed transactions
    └── products_active.parquet       # Active products only

Gold Layer (Business Aggregations)
    ├── customer_analytics.parquet    # Customer segment analysis
    ├── category_analytics.parquet    # Product category performance
    ├── payment_analytics.parquet     # Payment method usage
    ├── device_analytics.parquet      # Device/browser statistics
    └── product_analytics.parquet     # Product inventory analysis
```

## Features

- **Automated Data Generation**: Creates realistic sample datasets (10K customers, 50K transactions, 200 products)
- **Data Quality Checks**: Null value detection, duplicate analysis, completeness reports
- **Data Enrichment**: Calculated fields, customer segmentation, temporal features
- **Business Analytics**: Comprehensive aggregations for business insights
- **CI/CD Integration**: Automated deployment and execution via GitHub Actions
- **Logging**: Comprehensive logging at every pipeline stage

## CI/CD Workflow

### Automated Deployment

Every push to the `main` branch triggers:

1. **Environment Setup**: Ensures virtual environment with all dependencies
2. **Code Deployment**: Syncs latest code to OpenStack VM
3. **Pipeline Execution**: Runs the complete Spark pipeline
4. **Data Output**: Stores results in `~/pipeline-data/`

### Workflow File

See [`.github/workflows/deploy-and-run.yml`](.github/workflows/deploy-and-run.yml)

## Local Execution

### Prerequisites

- Python 3.11+
- Apache Spark 3.5.0
- Java 8
- Virtual environment with PySpark, findspark

### Setup

```bash
# Clone the repository
git clone https://github.com/jayachanders/dat535.git
cd dat535

# Activate Spark environment
source ~/spark-env/bin/activate

# Run the pipeline
python data_pipeline.py
```

### Configuration

The pipeline uses `DataPipelineConfig` class for configuration:

```python
config = DataPipelineConfig()
config.num_customers = 10000      # Adjust dataset size
config.num_transactions = 50000
config.num_products = 200
config.data_dir = "~/pipeline-data"  # Output location
```

## Data Output

All data is stored in Parquet format at:

```tree
~/pipeline-data/
├── bronze/       # Raw ingested data
├── silver/       # Cleaned and enriched data
└── gold/         # Business-level aggregations
```

## Pipeline Execution Flow

1. **Bronze Layer** (Raw Ingestion)
   - Generate sample datasets
   - Save as Parquet (schema-preserved, compressed)
   
2. **Silver Layer** (Cleansing & Enrichment)
   - Data quality checks
   - Customer segmentation
   - Filter completed transactions only
   - Active products filtering
   - Add computed columns

3. **Gold Layer** (Business Aggregations)
   - Customer segment analysis by city
   - Product category revenue analysis
   - Payment method statistics
   - Device/browser usage patterns
   - Product inventory by supplier

## Key Metrics

The pipeline generates the following business metrics:

- **Customer Segments**: Premium, Gold, Silver, Bronze (by spending)
- **Revenue Analysis**: By category, segment, payment method
- **Customer Lifetime Value**: Average order value, total spent
- **Inventory Analytics**: Stock levels by category and supplier
- **Channel Analytics**: Device/browser transaction patterns

## Logging

Comprehensive logging includes:
- Pipeline start/end timestamps
- Record counts at each layer
- Data quality check results
- Performance metrics
- Error handling with stack traces

Example output:
```
2026-01-14 12:00:00 - INFO - STARTING DATA PIPELINE EXECUTION
2026-01-14 12:00:05 - INFO - BRONZE LAYER: Raw Data Ingestion
2026-01-14 12:00:15 - INFO - ✓ Saved customers to: ~/pipeline-data/bronze/customers
2026-01-14 12:00:20 - INFO - SILVER LAYER: Data Cleansing and Enrichment
2026-01-14 12:00:35 - INFO - GOLD LAYER: Business Aggregations
2026-01-14 12:00:45 - INFO - PIPELINE EXECUTION COMPLETED SUCCESSFULLY
```

## Self-Hosted Runner Setup

This pipeline runs on a GitHub self-hosted runner on a private OpenStack VM:

```bash
# On OpenStack VM For production deployment:
cd ~/actions-runner
./config.sh --url https://github.com/jayachanders/dat535 --token YOUR_TOKEN
# This will not release the terminal, so no need to run
# ./run.sh

sudo ./svc.sh install
sudo ./svc.sh start
```

## Performance Considerations

- **Parquet Format**: Columnar storage for fast analytical queries
- **Partitioning**: Adjustable via `spark.sql.shuffle.partitions`
- **Adaptive Query Execution**: Enabled for dynamic optimization
- **Coalesce**: Used for small file consolidation

## Troubleshooting

### Virtual Environment Issues
If dependencies are missing, the workflow auto-recreates the venv:
```bash
rm -rf ~/spark-env
python3 -m venv ~/spark-env
```

### Check Pipeline Output
```bash
ls -lh ~/pipeline-data/bronze/
ls -lh ~/pipeline-data/silver/
ls -lh ~/pipeline-data/gold/
```

### Verify Data
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Verify").getOrCreate()

# Read Gold layer analytics
df = spark.read.parquet("~/pipeline-data/gold/customer_analytics")
df.show()
```

## Learning Objectives

This pipeline demonstrates:
- ✅ Medallion Architecture implementation
- ✅ Data quality and governance practices  
- ✅ Spark DataFrame API and SQL
- ✅ CI/CD for data pipelines
- ✅ Production logging and monitoring
- ✅ File format optimization (Parquet)
- ✅ Business analytics generation

## Next Steps

- Add incremental processing (date-based partitions)
- Implement data versioning (Delta Lake)
- Add data validation rules (Great Expectations)
- Create visualization dashboards (Superset/Grafana)
- Add alerting for data quality issues
- Implement CDC (Change Data Capture)

## License

Educational project for DAT535 course.
