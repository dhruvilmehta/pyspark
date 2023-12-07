from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

# Creating a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Reading the CSV file into a DataFrame
gcs_file_path = "gs://vernal-hall-403418/InputData/CrimeData.csv"
df = spark.read.csv(gcs_file_path, header=True, inferSchema=True)

# Grouping the DataFrame by 'Status Desc' and counting occurrences, ordering by count in descending order
status_counts = df.groupBy('Status Desc').count().orderBy('count', ascending=False)

# Displaying the counts for each 'Status Desc'
status_counts.show()

# Columns to drop from the DataFrame
cols_to_drop = ['DR_NO', 'Date Rptd', 'DATE OCC', 'TIME OCC', 'AREA', 'Rpt Dist No', 'Part 1-2', 'Crm Cd', 'Mocodes',
                'Premis Cd', 'Weapon Used Cd', 'Status', 'Crm Cd 1', 'Crm Cd 2', 'Crm Cd 3', 'Crm Cd 4', 'Cross Street',
                'LAT', 'LON', 'LOCATION']

# Dropping specified columns and removing rows with any missing values
df = df.drop(*cols_to_drop).na.drop()

# Categorical columns to be indexed
categorical_cols = ['AREA NAME', 'Crm Cd Desc', 'Vict Sex', 'Vict Descent', 'Premis Desc', 'Weapon Desc']

# Creating StringIndexer stages for each categorical column
indexers = [StringIndexer(inputCol=col, outputCol=col + "_index").fit(df) for col in categorical_cols]

# Creating a pipeline with the StringIndexer stages
pipeline = Pipeline(stages=indexers)

# Transforming the DataFrame with the pipeline to add index columns
df = pipeline.fit(df).transform(df)

# Feature columns for the VectorAssembler
feature_cols = ['Vict Age', 'AREA NAME_index', 'Crm Cd Desc_index', 'Vict Sex_index', 'Vict Descent_index',
                'Premis Desc_index', 'Weapon Desc_index']

# Assembling features using VectorAssembler
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df = assembler.transform(df)

# Indexing the label column using StringIndexer
label_indexer = StringIndexer(inputCol="Status Desc", outputCol="label").fit(df)
df = label_indexer.transform(df)

# Splitting the DataFrame into training and testing sets
(training_data, testing_data) = df.randomSplit([0.8, 0.2], seed=42)

# Creating a RandomForestClassifier
rf = RandomForestClassifier(labelCol="label", featuresCol="features", maxBins=500)

# Fitting the model on the training data
model = rf.fit(training_data)

# Making predictions on the testing data
predictions = model.transform(testing_data)

# Evaluating the accuracy of the model
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

# Printing the accuracy
print(f"Accuracy: {accuracy}")

# Saving the trained model to the specified path in Google Cloud Storage
model.save("gs://vernal-hall-403418/OutputModel/model")

from pyspark.mllib.evaluation import MulticlassMetrics
import matplotlib.pyplot as plt
import numpy as np

# Creating a DataFrame with custom input data
custom_input_data = [
    (71, 'Central', 'ASSAULT WITH DEADLY WEAPON, AGGRAVATED ASSAULT', 'M', 'W', 'PUBLIC RESTROOM/OUTSIDE*', 'UNKNOWN WEAPON/OTHER WEAPON')
]

columns = ['Vict Age', 'AREA NAME', 'Crm Cd Desc', 'Vict Sex', 'Vict Descent', 'Premis Desc', 'Weapon Desc']
custom_df = spark.createDataFrame(custom_input_data, columns)

# Transforming categorical columns to numerical indices using the previously fitted StringIndexers
custom_df = pipeline.fit(df).transform(custom_df)

# Assembling features for the custom input data
feature_cols = ['Vict Age', 'AREA NAME_index', 'Crm Cd Desc_index', 'Vict Sex_index', 'Vict Descent_index',
                'Premis Desc_index', 'Weapon Desc_index']

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
custom_df = assembler.transform(custom_df)

# Displaying the DataFrame with features
custom_df

from pyspark.ml.classification import RandomForestClassificationModel
model = RandomForestClassificationModel.load("gs://vernal-hall-403418/OutputModel/model")

# Making predictions using the loaded RandomForestClassificationModel on the custom input data
predictions = model.transform(custom_df)

# Displaying specific columns from the predictions DataFrame
predictions.select("features", "prediction", "probability").show()

# Obtaining the labels used by the StringIndexer for 'Status Desc'
status_desc_labels = label_indexer.labels

# Displaying the mapping of 'Status Desc' values to encoded values
print("Mapping of Status Desc values to encoded values:")
for label, encoded_value in enumerate(status_desc_labels):
    print(f"{encoded_value}: {label}")