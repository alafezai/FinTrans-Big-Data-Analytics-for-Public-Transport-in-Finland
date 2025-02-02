from pyspark.sql.functions import col, hour, minute, second
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.regression import GeneralizedLinearRegression # ✅ Changed to LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator  # ✅ Evaluator for regression
from pyspark.sql import SparkSession
import os

# ✅ Start Spark session with optimized configurations
print("Starting Spark Session...")
spark = SparkSession.builder \
    .appName("OptimizedMLPipeline") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.3") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.default.parallelism", "2") \
    .getOrCreate()

# ✅ Load data with sampling
print("Loading dataset...")
data_path = "/mnt/c/Users/me/OneDrive/Bureau/merged_df_prediction/merged_df.csv"
data = spark.read.csv(data_path, header=True, inferSchema=True)
data = data.sample(fraction=0.001, seed=42)  # Reduce dataset size
print(f"Dataset loaded with {data.count()} rows")

# ✅ Handle missing values
data = data.fillna({"spd": 0, "vehicle_type": "Unknown", "route_id": "Unknown", "dl": 0})  # Fixed missing delay values
print("Missing values filled")

# ✅ Rename 'dl' to 'label' for training
data = data.withColumnRenamed("dl", "label")

# ✅ Ensure 'label' is numeric
data = data.withColumn("label", col("label").cast("float"))

# ✅ Extract time features
if 'start_time' in data.columns:
    print("Extracting time features from start_time...")
    data = data.withColumn("hour", hour(col("start_time"))) \
               .withColumn("minute", minute(col("start_time"))) \
               .withColumn("second", second(col("start_time")))
elif 'arrival_time' in data.columns:
    print("Extracting time features from arrival_time...")
    data = data.withColumn("hour", hour(col("arrival_time"))) \
               .withColumn("minute", minute(col("arrival_time"))) \
               .withColumn("second", second(col("arrival_time")))

# Fill any remaining NULLs in new time columns
data = data.fillna({"hour": 0, "minute": 0, "second": 0})
print("Time features extracted")

# ✅ Convert categorical columns to numerical
print("Indexing categorical columns...")
indexers = [
    StringIndexer(inputCol=col, outputCol=col+"_index", handleInvalid="keep").fit(data) 
    for col in ["vehicle_type", "route_id"]
]

# ✅ Assemble features
print("Assembling feature vector...")
feature_cols = ["vehicle_type_index", "route_id_index", "spd", "hour", "minute", "second"]
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")

# ✅ Normalize features
print("Scaling features...")
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

# ✅ Use Linear Regression for continuous target variable
print("Training Linear Regression model...")
# Ridge regression instead of standard Linear Regression
lr = GeneralizedLinearRegression(featuresCol="scaled_features", labelCol="label", regParam=0.1)

# ✅ Create pipeline
pipeline = Pipeline(stages=indexers + [vector_assembler, scaler, lr])

# ✅ Train-test split
print("Splitting dataset...")
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
print(f"Training data: {train_data.count()} rows, Test data: {test_data.count()} rows")

# ✅ Train model
print("Fitting model...")
model = pipeline.fit(train_data)
print("Model training complete")

# ✅ Save the trained model
model_path = os.path.join(os.path.dirname(data_path), "lr_model")
model.save(model_path)
print(f"Model saved at {model_path}")

# ✅ Make predictions
print("Generating predictions...")
predictions = model.transform(test_data)
print("Predictions complete")

# ✅ Evaluate model using RMSE
print("Evaluating model performance...")
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")

# ✅ Stop Spark session
print("Stopping Spark Session...")
spark.stop()
print("Execution complete")