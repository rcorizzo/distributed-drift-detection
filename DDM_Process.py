# Settings
import sys
from random import choice

# Preset Arguments
URL = "spark://***.***.***.***:7077"
INSTANCES = "10"
CORES = "4"
MEMORY = "8g"

FILENAME = "outdoorStream.csv"
TIME_STRING = "Placeholder" # Used to index runs.  Filled with placeholder because all runs used the command line arguments below
MULT_DATA = 2

# CLI Arguments
# Only uncomment if running with command-line arguments
# Format: python DDM_Process.py URL INSTANCES MEMORY CORES TIME_STRING MULT_DATA
# URL = sys.argv[1]
# INSTANCES, MEMORY, CORES = sys.argv[2], sys.argv[3], sys.argv[4]
# TIME_STRING = sys.argv[5]
# MULT_DATA = sys.argv[6]

APP_NAME = "%s-%s" % (FILENAME, TIME_STRING)

PER_BATCH = 100

MIN_NUM_DDM_VALS = 3
WARNING_LEVEL = 0.5
CHANGE_LEVEL = 1.5

REGRESSION_THRESH = 0.3

NUMBER_OF_FEATURES = 27 # Set based on data file
X_features =[str(i) for i in range(NUMBER_OF_FEATURES)]
y_true = ["target"]


# Load dataset
import pandas as pd
import numpy as np

df = pd.read_csv(FILENAME)

if float(MULT_DATA) < 1:
    print("Float sampling")
    df = df.sample(frac=float(MULT_DATA))
else:
    print("Duplicating data")
    df = pd.concat([df] * int(float(MULT_DATA))).sample(frac=1)

df = df.sort_values(by="target")

num_rows = len(df)
number_of_changes = len(df["target"].unique())
dist_between_changes = num_rows//number_of_changes


# Open Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder.master(URL).appName("DDM-%s" % APP_NAME)
spark.config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
spark.config("spark.dynamicAllocation.enabled", "true")
spark.config("spark.dynamicAllocation.maxExecutors", INSTANCES)
spark.config("spark.dynamicAllocation.minExecutors", INSTANCES)
spark.config("spark.executor.cores", CORES)
spark.config("spark.executor.memory", MEMORY)
spark.config("spark.driver.cores", "8")
spark.config("spark.driver.memory", "32g")
spark.config("spark.rpc.message.maxSize", "512")

spark = spark.getOrCreate()


# ## Process Overview

# ### Pre Loop Process
# 1. Timer starts
# 2. DF is segmented based on the number of devices
# 
# ### DDM Loop Process
# 
# 1. Train on batch a
# 2. Predict on batch b
# 3. Use DDM to evaluate predictions
#  * If change is detected, DDM reports where and the process restarts with batch b becoming batch a and the next available batch becoming batch b
#  * If no change is detected, DDM reports -1 and the process restarts from step 2, with the next available batch becoming batch b
# 
# ### Post Loop Process
# 
# 1. All changes from all devices are grouped together with the expectation that every device will find the same changes in each batch
# 2. The actual changes are used to calculate the average delay

# DDM Loop Process
# Training function
from sklearn.ensemble import RandomForestClassifier 

def train_rf(df_a: pd.DataFrame) -> RandomForestClassifier:
    X = df_a[X_features].values
    y = df_a[y_true].values[:, 0]
    
    rf = RandomForestClassifier(n_jobs=int(CORES))
    rf.fit(X, y)
    
    return rf


# DDM Loop Process
# Prediction function
def predict_rf(df_b: pd.DataFrame, rf: RandomForestClassifier) -> pd.DataFrame:
    X = df_b[X_features].values
    y = df_b[y_true].values[:, 0]
    
    y_pred = rf.predict(X)
    
    # True means error for the DDM
    acc = y_pred != y
            
    df_res = pd.DataFrame({
        "y_true": y,
        "y_pred": y_pred,
        "accuracy": acc,
        "full_df_row_number": df_b["full_df_row_number"]
    }, index=df_b.index)
    
    df_res["accuracy"] = df_res["accuracy"].astype(int)
    
    return df_res


# DDM Loop Process
# DDM function
from skmultiflow.drift_detection import DDM

def run_DDM(df_b: pd.DataFrame, prev_ddm: DDM = None) -> (pd.DataFrame, DDM):
    if prev_ddm:
        ddm = prev_ddm
    else:
        ddm = DDM(min_num_instances=MIN_NUM_DDM_VALS, warning_level=WARNING_LEVEL, out_control_level=CHANGE_LEVEL)
    
    earliest_warning = (-1, -1)
    earliest_change = (-1, -1)
    
    for i, sample in df_b.iterrows():
        ddm.add_element(sample["accuracy"])
        
        if ddm.detected_warning_zone() and earliest_warning == (-1, -1):
            earliest_warning = (i, sample["full_df_row_number"])
            
        if ddm.detected_change():
            earliest_change = (i, sample["full_df_row_number"])
            break
    
    return (pd.DataFrame({
        "warning_flag_local": earliest_warning[0],
        "warning_flag_global": earliest_warning[1],
        "change_flag_local": earliest_change[0],
        "change_flag_global": earliest_change[1]
    }, index=[0]), ddm)


# DDM Loop Process
# Overarching Function
from pyspark.sql.functions import col, when, pandas_udf, PandasUDFType, udf

@pandas_udf(
    "warning_flag_local int, warning_flag_global int, change_flag_local int, change_flag_global int",
   PandasUDFType.GROUPED_MAP
)
def run_DDM_loop(df: pd.DataFrame) -> pd.DataFrame:
    # Create results dataframe and variables
    results = pd.DataFrame({
        "warning_flag_local": [],
        "warning_flag_global": [],
        "change_flag_local": [],
        "change_flag_global": []
    })
    ddm = None
    retrain = True
    
    # Batch dataframe: https://stackoverflow.com/a/49415851
    batches = []
    for start in range(0, df.shape[0], PER_BATCH):
        batches.append(df.iloc[start:start + PER_BATCH])
    
    # Run loop
    batch_a = batches[0].sample(frac=1)
    results = []
    for batch_b in batches[1:]:
        batch_b = batch_b.sample(frac=1)
        # print(batch_b)
        
        # Train RF
        if retrain:
            rf = train_rf(batch_a)
            retrain = False
        
        # Apply RF
        predictions = predict_rf(batch_b, rf)
        
        # Run DDM
        changes, ddm = run_DDM(predictions, prev_ddm=ddm)
        # print(changes)
        results.append(changes)        
        
        # Process DDM response
        if changes["change_flag_global"].values[0] > -1:
            batch_a = batch_b
            ddm = None
            retrain = True

    results = pd.concat(results)
    return results


# Pre Loop Process
# Call DDM Process Loop
from time import time

df["full_df_row_number"] = df.index
# df["device_id"] = df["full_df_row_number"].values % int(INSTANCES)
df = spark.createDataFrame(df)

start_time = time()
df = df.withColumn("device_id", udf(lambda x: x % int(INSTANCES), "int")(df.full_df_row_number))
df = df.repartition("device_id").groupby("device_id").apply(run_DDM_loop)


# Post Loop Process
# Collect all change indexes
from pyspark.sql.functions import lit

@pandas_udf(
    "changes int",
    PandasUDFType.GROUPED_MAP
)
def collect_change_indexes(df: pd.DataFrame) -> pd.DataFrame:
    # Setup final variables
    changes = []
    
    for _, sample in df.iterrows():
        if sample["change_flag_global"] > -1:
            changes.append(sample["change_flag_global"])
    
    return pd.DataFrame(
        {"changes": changes},
        index=[i for i in range(len(changes))]
    )


# Post Loop Process
# Find distances to actual change
@pandas_udf("int")
def calc_change_dist(changes: pd.Series) -> pd.Series:
    # Setup final variables
    return changes % dist_between_changes

df = df.withColumn('distance', calc_change_dist(df.change_flag_global)).toPandas()
df = df.where(df["change_flag_global"]!=-1).dropna()
total_time = time() - start_time


# Post Loop Process
# Save to files
try:
    passed_runs = pd.read_csv("ddm_cluster_runs.csv", index_col=0).values.tolist()
except (FileNotFoundError, pd.errors.EmptyDataError):
    passed_runs = []

pd.DataFrame(
    passed_runs + [(APP_NAME, TIME_STRING, URL, int(INSTANCES), float(MULT_DATA), MEMORY, int(CORES), total_time, df["distance"].mean())],
    columns = ["Spark App", "Exp Start Time", "Spark Address", "Instances", "Data Multiplier", "Memory", "Cores", "Final Time", "Average Distance"]
).to_csv("sparse_cluster_runs.csv")
