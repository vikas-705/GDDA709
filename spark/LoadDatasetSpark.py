from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TimeSeriesPrediction").getOrCreate()
data = spark.read.csv("/data/input/cleaned_dataset.csv", header=True, inferSchema=True)
data.show()
