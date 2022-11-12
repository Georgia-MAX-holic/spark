from pyspark.sql.types import *


#스트리밍 쿼리
# 1단계 : 입력 소스 지정

spark = SparkSession #...(생략)
lines = (spark
        .readStram.format("socket")
        .option("host","localhost")
        .option("port", 9999)
        .load()
        )

#2단계 : 데이터 변형 / 개별 단어로 나눈다던지, 개수를 세능 등의 일상적인 데이터 프레임 연산을 수행 가능
words = lines.select(split(col("value"), "\\s").alias("word") )
counts = words.groupBy("word").count()

# 출력 싱크와 모드 결정  
#자세한 출력 방식 (어디에 어떻게 출력 결과가 쓰일지 )
#자세한 처리 방식 ( 어떻게 데이터가 처리되고, 장애시 어떻게 복구되는지 )

writer = counts.writeStream.format("console").outputMode("complete")


#4단계 : 처리 세부사항 지정 


checkpointdir ="..."
writer2 = (writer
          .trigger(processingTime="1 second")
          .option("checkpointLocation", checkpointdir))

#5단계 : 쿼리 시작 

StreamingQuery = writer2.start()

#종합적 예제 

from pyspark.sql.functions import * 
spark = SparkSession..
lines =(spark
        .readStream.format("socket")
        .option("host","localhost")
        .option("port",9999)
        .load()
)

words = lines.select(split(col("value"),"\\s").alias("word"))
counts = words.groupBy("word").count()
checkpointdir = "..."
streamingQuery = (counts
                 .writeStream
                 .format("console")
                 .outputMode("complete")
                 .trigger(processingTime="1 second")
                 .option("checkpointLocation", checkpointdir)
                 .start())

streamingQuery.awaitTermination()



#파일에서 읽기

from pyspark.sql.types import * 
inputDirectoryOfJsonFiles = ...

fileSchema = (StrucType()
             .add(StrucField("key",IntegerType()))
             .add(StrucField("value", IntegerType())))

inputDF = (spark
          .readStream
          .format("json")
          .chema(fileSchema)
          .load(inputDirectoryOfJsonFiles)
          )
