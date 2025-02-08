from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=["Humidity", "WindSpeed"], outputCol="features")
data = assembler.transform(data)
