from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol="features", labelCol="Temperature")
model = lr.fit(data)
predictions = model.transform(data)
predictions.show()
