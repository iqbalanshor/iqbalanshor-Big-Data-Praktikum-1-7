# module_5_stats.py
# Big Data Practicum - Module 5: Descriptive Statistics
# Environment: Google Colab / Jupyter Notebook

import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, skewness, variance, mean, stddev, min, max

# Initialize Spark Session
spark = SparkSession.builder.appName("Module5_DescriptiveStatistics").getOrCreate()

# 1. Load 'diamonds' dataset using Seaborn and convert to Spark DataFrame
print("Loading diamonds dataset...")
diamonds_pdf = sns.load_dataset('diamonds')
diamonds_df = spark.createDataFrame(diamonds_pdf)

print("Schema:")
diamonds_df.printSchema()
print("First 5 rows:")
diamonds_df.show(5)

# 2. Calculate Descriptive Statistics

# 2.a. Using .describe() for basic stats
print("\n--- Basic Statistics (describe) ---")
diamonds_df.select("carat", "depth", "price").describe().show()

# 2.b. Using pyspark.sql.functions for specific stats (Skewness, Variance)
print("\n--- Specific Statistics using pyspark.sql.functions ---")
diamonds_df.select(
    skewness("price").alias("price_skewness"),
    variance("price").alias("price_variance"),
    mean("price").alias("price_mean"),
    stddev("price").alias("price_stddev")
).show()

# 2.c. Median Calculation Comparison
print("\n--- Median Calculation Comparison ---")

# Method 1: ApproxQuantile with 0.01 error (Faster, approximate)
# The second argument 0.5 requests the 50th percentile (median).
# The third argument 0.01 is the relative error allowed.
median_approx_fast = diamonds_df.stat.approxQuantile("price", [0.5], 0.01)[0]
print(f"Median (approxQuantile, error=0.01): {median_approx_fast}")

# Method 2: ApproxQuantile with 0.0 error (More accurate, stricter)
# Setting error to 0.0 forces an exact calculation for the quantile if possible, 
# but can be very expensive on large datasets.
median_approx_exact = diamonds_df.stat.approxQuantile("price", [0.5], 0.0)[0]
print(f"Median (approxQuantile, error=0.0): {median_approx_exact}")

# Note: .summary() includes '50%' which corresponds to the median
print("\n--- Summary (includes 50%) ---")
diamonds_df.select("price").summary("50%").show()


# 3. Visualization
# Sample 10% of the data and convert to Pandas for plotting
print("\n--- Visualization (10% Sample) ---")
sample_df = diamonds_df.sample(withReplacement=False, fraction=0.1, seed=42).toPandas()

# 3.a. Histogram (Distribution)
plt.figure(figsize=(10, 5))
sns.histplot(sample_df['price'], kde=True, color='blue')
plt.title('Price Distribution (Histogram) - 10% Sample')
plt.xlabel('Price')
plt.ylabel('Frequency')
plt.show()

# 3.b. Box Plot (Outliers)
plt.figure(figsize=(10, 5))
sns.boxplot(x=sample_df['price'], color='orange')
plt.title('Price Box Plot - 10% Sample')
plt.show()


# 4. Solved Exercises

print("\n--- Solved Exercises ---")

# Exercise 1: Stats for 'carat'
print("Stats for 'carat':")
diamonds_df.select(
    mean("carat").alias("mean_carat"),
    stddev("carat").alias("stddev_carat"),
    min("carat").alias("min_carat"),
    max("carat").alias("max_carat")
).show()

# Exercise 2: Compare avg price by color
print("Average Price by Color:")
diamonds_df.groupBy("color").avg("price").orderBy("color").show()

# Exercise 3: Check 'depth' distribution (skewness)
print("Skewness of 'depth':")
diamonds_df.select(skewness("depth").alias("depth_skewness")).show()

# Stop Spark Session
spark.stop()
