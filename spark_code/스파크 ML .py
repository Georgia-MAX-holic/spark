
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

#파이프라인 생성 

from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[vecAssembler , lr])
pipelineModel = pipeline.fit(trainDF)

predDF = pipelineModel.transform(testDF)
predDF.select("bedrooms","features", "price","prediction").show(10)

#원핫인코딩

from pyspark.ml.feature import OneHotEncoder, stringIndexer

categoricalCols = [field for (field, dataType) in trainDF.dtypes
                  if dataType =="string"]

indexOutputCols = [x + "Index" for x in categoricalCols]

stringIndexer = StringIndexer (inputCols = categoricalCols,
                              outputCols = indexOutputCols,
                              handleInvalid="skip")

oheEncoder = OneHotEncoder(inputCols = indexOutputCols,
                          outputCols = oheOutputCols)

numericCols = [field for (field,dataType) in trainDF.dtypes
              if ((dataType =="double") & (field !="price"))]

assemblerInputs = oheOutputCols + numbericCols

vecAssembler = VectorAssembler(inputCols=assemblerInputs,
                              outputCol = "features")

#RForbula
#윗내용이랑 같은 효과

from pyspark.ml.feature import RFormula

rFormula = RFormula(formula="price ~ .",
                   featuresCol="features",
                   labelCol="price",
                   handelInvalid="skip")

# 다 하고 나면 선형 사용 가능 

lr = LinearRegression(lableCol="price", featuresCol="features")
pipeline = Pipeline(stages = [stringIndexer, oheEncoder, vecAssembler, lr])
#Ehsms RFormula
#pipeline = Pipeline(stages = [rFormula,lr])

pipelineModel = pipeline.fit(trainDF)
predDF = pipelineMode.transfor(testDF)
predDF.select("features", "price", "prediction").show(5)

#모델 평가
from pyspark.ml.evluation import RegressionEvaluator
RegressionEvaluator= RegressionEvaluator(
   predictionCol="prediction",
   labelCol="price",
   metricName="rmse")

rmse = regressionEvaluator.evaluate(predDF)
print(f"RMSE is {rmse.1f}")

#모델 저장 
pipelinePath = "/tmp/lr-pipeline-model"
pipelineModel.write().overwrite().save(pipelinePath)

#모델 로드 / 모델할때는 로드할 모델을 자시 지정해야함
from pyspark.ml import PipelineModels
savedPipelineModel = PipelineModel.load(pipelinePath)


#하이퍼파라미터 튜닝 

#결정트리
from pyspark.ml.regression import DecisionTreeRegressor

dt = DecisionTreeRegressor(labelCol="price")

#숫자 열만 필터링(가격,레이블 제외)
numbericCols =[field for (field, dataType) in trainDF.dtypes
              if ((dataType =="double") & (field != "price"))]

#위에서 정의한 StringIndexer의 출력과 숫자 열 결합 
assemblerInputs =indexOutputCols +numbericCols
vecAssembler =VectorAssembler(inputCols=assemblerInputs, outputCol="features")

#단계를 파이프라인으로 결합
stages =[stringIndexer, vecAssembler, dt]
pipeline Pipeline(stages=stages)
pipelineModel = pipeline.fit(trainDF) # 여기서 에러 발생 

# setMaxBins() 메서드를 사용하여 결정
dt.setMaxBins(40)
pipelineModel =pipeline.fit(trainDF)

#가장 중요한 기능을 보기 위해 모델에서 기능 중요도 점수를 추출 

import pandas as pd 

featureImp = pd.Dataframe(
list(zip(vecAssembler.getInputCols(), dtModel.featureImportances)),
columns=["feature", "importance"])

featureImp.sort_values(by="importance", ascending=False)



