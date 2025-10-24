# run_analysis.py (Includes findspark Fix)

import os
import sys
# 1. ADD findspark import
import findspark 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# --- Configuration (Adjust if needed) ---
APP_NAME = "SimpleSDG10Analysis"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "data", "census_data.csv")
OUTPUT_STATS_FILE = os.path.join(BASE_DIR, "results", "sdg10_income_stats.csv")
OUTPUT_FIG_PATH = os.path.join(BASE_DIR, "results", "sdg10_income_bar.png")

# --- Assumed Data Columns (MUST match your CSV) ---
INCOME_COL = "income"
CURRENT_REGION_COL = "destination"
PREVIOUS_REGION_COL = "origin"


def init_spark_session(app_name):
    """Initializes and returns a PySpark SparkSession using findspark."""
    # 2. CRITICAL FIX: Initialize findspark 
    findspark.init() 
    
    print("Starting Spark Session...")
    try:
        # Using local[*] to run on all cores for max speed
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()
        return spark
    except Exception as e:
        print(f"🚨 Spark Initialization Failed. Check Java/PySpark setup: {e}", file=sys.stderr)
        return None

def run_pyspark_pipeline(spark):
    """Runs the full PySpark ETL and SDG 10 analysis."""
    if not spark:
        return

    # --- 1. Data Ingestion (Big Data Read) ---
    print(f"Loading data from: {INPUT_FILE}")
    try:
        df = spark.read.csv(INPUT_FILE, header=True, inferSchema=True)
        print(f"Successfully loaded {df.count()} records.")
    except Exception as e:
        print(f"🚨 ERROR: Could not load data. Ensure '{INPUT_FILE}' exists and is correct CSV format: {e}", file=sys.stderr)
        return

    # --- 2. Transformation & Feature Engineering (Distributed) ---
    # a) Type casting and cleaning
    df_clean = df.withColumn(INCOME_COL, F.col(INCOME_COL).cast(DoubleType())) \
        .filter(F.col(INCOME_COL).isNotNull() & (F.col(INCOME_COL) > 0))

    # b) Key Feature: Define Migrant Status (Distributed Transformation)
    df_with_status = df_clean.withColumn(
        "MIGRANT_STATUS", 
        F.when(F.col(CURRENT_REGION_COL) != F.col(PREVIOUS_REGION_COL), "Migrant")
         .otherwise("Non-Migrant")
    )
    
    # --- 3. Analysis (SDG 10: Income Inequality) ---
    print("Calculating average income inequality by migration status...")
    
    # Distributed aggregation (The core Big Data operation)
    income_stats_spark = df_with_status.groupBy("MIGRANT_STATUS") \
        .agg(
            F.mean(INCOME_COL).alias("Average_Income"),
            F.count("*").alias("Total_Count")
        ) \
        .orderBy("MIGRANT_STATUS", ascending=False)
    
    # --- 4. Result Extraction and Visualization ---
    
    # Convert the small aggregated result to Pandas for plotting
    stats_pd = income_stats_spark.toPandas()

    # Save the aggregated results
    os.makedirs(os.path.dirname(OUTPUT_STATS_FILE), exist_ok=True)
    stats_pd.to_csv(OUTPUT_STATS_FILE, index=False)
    print(f"\n✅ Aggregated stats saved to: {OUTPUT_STATS_FILE}")
    print("Summary:\n", stats_pd)
    
    # Visualization (SDG 10 focus)
    plt.figure(figsize=(8, 6))
    sns.barplot(
        x='MIGRANT_STATUS', 
        y='Average_Income', 
        data=stats_pd, 
        palette={'Migrant': 'skyblue', 'Non-Migrant': 'coral'}
    )
    plt.title('Average Income: Migrants vs. Non-Migrants (SDG 10)', fontsize=14)
    plt.ylabel('Average Income (USD)', fontsize=12)
    plt.xlabel('Migration Status', fontsize=12)
    
    # Save the figure
    os.makedirs(os.path.dirname(OUTPUT_FIG_PATH), exist_ok=True)
    plt.savefig(OUTPUT_FIG_PATH)
    print(f"✅ Visualization saved to: {OUTPUT_FIG_PATH}")


# --- Execution Block ---
if __name__ == "__main__":
    
    # Ensure results directory exists for output
    os.makedirs(os.path.join(BASE_DIR, "results"), exist_ok=True)

    # CRITICAL: Create a dummy data file if it doesn't exist for testing
    if not os.path.exists(INPUT_FILE):
        print(f"!!! CRITICAL: Creating dummy data file at: {INPUT_FILE}")
        os.makedirs(os.path.dirname(INPUT_FILE), exist_ok=True)
        
        # Data reflecting: origin/destination (migration) and income (inequality)
        dummy_data = {
            'person_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'destination': ['NY', 'CA', 'NY', 'TX', 'CA', 'TX', 'NY', 'CA', 'TX', 'TX'],
            'origin': ['NY', 'CA', 'CA', 'NY', 'TX', 'TX', 'NY', 'NY', 'TX', 'CA'],
            'income': [70000, 80000, 50000, 95000, 60000, 75000, 85000, 55000, 88000, 62000]
        }
        df_dummy = pd.DataFrame(dummy_data)
        df_dummy.to_csv(INPUT_FILE, index=False)
        print("!!! Dummy data created. Running analysis on simplified data.")


    spark = init_spark_session(APP_NAME)
    
    run_pyspark_pipeline(spark)
    
    if spark:
        spark.stop()
    print("\nProject execution finished.")
