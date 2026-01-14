import findspark
import os

# Initialize findspark with your OpenStack Spark path
findspark.init(spark_home='/opt/spark')

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("GitHub-Triggered-Self-Hosted-Runner-Pipeline") \
        .getOrCreate()

    print("Pipeline running locally on Private VM on OpenStack via GitHub Runner")
    
    # Simple test data
    data = [("Deployment", 1), ("Execution", 2), ("Success", 3)]
    df = spark.createDataFrame(data, ["Step", "Status"])
    df.show()

    # Example logic
    df2 = spark.createDataFrame([("Private_IP", "Success")], ["Network", "Status"])
    df2.show()

    spark.stop()