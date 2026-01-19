# module_6_mllib.py
# Big Data Practicum - Module 6: Spark MLlib (Machine Learning)
# Environment: Google Colab / Jupyter Notebook

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col

# Initialize Spark Session
spark = SparkSession.builder.appName("Module6_MLlib").getOrCreate()

print("=== Spark MLlib Practicum ===\n")

# -----------------------------------------------------------------------------
# 1. Regression: Predict Salary based on Experience & Age
# -----------------------------------------------------------------------------
print("--- 1. Linear Regression (Predict Salary) ---")

# Dummy Data: Salary, Experience (years), Age
data_reg = [
    (30000, 1, 22),
    (35000, 2, 23),
    (40000, 3, 24),
    (50000, 5, 26),
    (60000, 6, 28),
    (75000, 8, 30),
    (90000, 10, 35),
    (100000, 12, 38),
]
columns_reg = ["Salary", "Experience", "Age"]
df_reg = spark.createDataFrame(data_reg, columns_reg)

# Feature Engineering
assembler_reg = VectorAssembler(inputCols=["Experience", "Age"], outputCol="features")
final_data_reg = assembler_reg.transform(df_reg).select("features", "Salary")

# Train Model
lr = LinearRegression(labelCol="Salary", featuresCol="features")
lr_model = lr.fit(final_data_reg)

print(f"Coefficients: {lr_model.coefficients}")
print(f"Intercept: {lr_model.intercept}")
print(f"RMSE: {lr_model.summary.rootMeanSquaredError}")

# Prediction on new data
test_data_reg = spark.createDataFrame([(4, 25), (15, 40)], ["Experience", "Age"])
test_features_reg = assembler_reg.transform(test_data_reg)
predictions_reg = lr_model.transform(test_features_reg)
print("\nPredictions for new data:")
predictions_reg.select("Experience", "Age", "prediction").show()


# -----------------------------------------------------------------------------
# 2. Classification: Predict Churn based on Usage Duration & Complaints
# -----------------------------------------------------------------------------
print("\n--- 2. Logistic Regression (Predict Churn) ---")

# Dummy Data: Churn (1=Yes, 0=No), Usage_Duration (hours), Complaints (count)
data_clf = [
    (0, 50, 0),
    (0, 60, 1),
    (0, 45, 0),
    (1, 10, 5),
    (1, 15, 3),
    (1, 5, 4),
    (0, 80, 0),
    (1, 20, 6),
]
columns_clf = ["Churn", "Usage_Duration", "Complaints"]
df_clf = spark.createDataFrame(data_clf, columns_clf)

# Feature Engineering
assembler_clf = VectorAssembler(
    inputCols=["Usage_Duration", "Complaints"], outputCol="features"
)
final_data_clf = assembler_clf.transform(df_clf).select("features", "Churn")

# Train Model
logr = LogisticRegression(labelCol="Churn", featuresCol="features")
logr_model = logr.fit(final_data_clf)

print("Coefficients: " + str(logr_model.coefficients))
print("Intercept: " + str(logr_model.intercept))

# Predict
print("\nPredictions on Training Data (Validation):")
predictions_clf = logr_model.transform(final_data_clf)
predictions_clf.select("features", "Churn", "prediction", "probability").show()


# -----------------------------------------------------------------------------
# 3. Clustering: Group Mall Customers based on Income & Score
# -----------------------------------------------------------------------------
print("\n--- 3. KMeans Clustering (Mall Customers) ---")

# Dummy Data: CustomerID, Annual_Income (k$), Spending_Score (1-100)
data_cluster = [
    (1, 15, 39),
    (2, 15, 81),
    (3, 16, 6),
    (4, 16, 77),
    (5, 17, 40),
    (6, 17, 76),
    (7, 18, 6),
    (8, 18, 94),
    (9, 19, 3),
    (10, 19, 72),
    (11, 20, 15),
    (12, 20, 79),
]
columns_cluster = ["CustomerID", "Income", "Score"]
df_cluster = spark.createDataFrame(data_cluster, columns_cluster)

# Feature Engineering
assembler_cluster = VectorAssembler(inputCols=["Income", "Score"], outputCol="features")
final_data_cluster = assembler_cluster.transform(df_cluster).select(
    "CustomerID", "features"
)

# Train Model (K=3)
kmeans = KMeans(k=3, seed=1)
kmeans_model = kmeans.fit(final_data_cluster)

# Cluster Centers
centers = kmeans_model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

# Predict Clusters
predictions_cluster = kmeans_model.transform(final_data_cluster)
print("\nCluster Assignments:")
predictions_cluster.select("CustomerID", "features", "prediction").show()

# Stop Spark Session
spark.stop()
