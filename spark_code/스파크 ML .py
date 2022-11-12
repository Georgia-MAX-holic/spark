
#스파크 ML
#셋팅
filePath = """ /databricks-datasets/learning-spark-v2/sf-airbnb/
sf-airbnb-clean.parquet/
"""
airbnbDF =spark.read.parquet(filePath)
airbnbDF.select("neighbourhood_cleansed","room_type","bathrooms", "number_of_reviews","price").show(5)

# Test, Train 나누기
TrainDF, testDF = airbnbDF.randomSplit([.8,.2],seed=42)
print(f"""There are {trainDF.count()} rows in the training set,
and {testDF.count()} in the test set
""")

#변환기를 이용하여 기능 준비 

from pyspark.ml.feature import VectorAssembler
vecAssembler =VectorAssembler(inputCols=["bedrooms"], outputCol="features")
vecTrainDF = vecAssembler.transform(trainDF)
vecTrainDF.select("bedrooms", "features", "price").show(10)

# 선형회귀 (LinearRegression)

from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol="features", labledCol="price")
lrModel = lr.fit(vecTrainDF)

#학습한 매개 변수 확인

m = round(lrModel.coefficients[0],2)
b = round(lrModel.intercept, 2)
print(f"""The formula for the linear regression line is
price =$m%1.2f*bedrooms + $b%1.2f""")

