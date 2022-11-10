# 파이썬에서 스키마를 정의함 
from pyspark.sql.types import *

# 프로그래밍적인 방식으로 스키마를 정의 

fire_schema = StructType([StructField("CallNumber"), IntegerType(), True,
              StructType("UnitId", StringType(),true),
              StructType("IncidentNumber", IntegerType(),true),
              StructType("CallDate", StringType(),true),
              StructType("WatchDate", StringType(),true),
#            (생략)
                         ])

#DataFrameReader

sf_fire_file = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-call.csv" 
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema) 


# 파케이 저장 

parquet_path = ... 

fire_df.write.format("parquet").save(parquet_path)


#메타스토어에 메타데이터로 등록되는 테이블로 저장 

parquet_table=...
fire_df.write.format("parquet").saveAsTable(parquet_table)

#프로젝션과 필터 (프로젝션 : 필터를 이용해 특정 관계 상태와 매치되는 행들만 되돌려 주는 방법)

few_fire_df = (fire_df
               .select("IncidentNumber", "AvailableDtTm", "CallType")
               .where(col("CallType") !="Medical Incident"))

#fire_df 의 "IncidentNumber", "AvailableDtTm", "CallType"만 추출, 그리고 CallType의 열에서 Medical Incident가 아닌것들을 추출 
few_fire_df.show(5, truncate=False)
# 5개만 


#모든 행에서 null이 아닌 개별 CallType을 추출

fire_df.select("CallType").where(cal("CallType").isNotNull().distinct().show(1, False))

 
 #컬럼 이름 변경, 추가 ,삭제 
 
new_fire_df = fire_df.withColumnRenamed("Delay", "responseDelayedinMins")  
# 이름 변경  Delay => res~~~
 
new_fire_df.select("ResponseDelayedinMins").where(col("ResponseDelayedinMins")>5).show(5,False)

                                 
#타입 변경
                                 
fire_ts_df = (new_fire_df
             .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))# to_timestamp() , to_date() , 날짜 변환 함수들 
             .drop("CallDate")
             .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
             .drop("WatchDate")
              .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ssa"))
             .drop("AvailableDtTm"))

#변환된 칼럼들을 가져온다 
(fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False))
            

    
#groupBy() , orderBy(), count() 와 같이 데이터 프레임에서 쓰는 일부 트랜스포메이션과 액션은 칼럼이름으로 집계해서 각각 개수를 세어주는 기능을 제공 
#가장 흔한 형태의 신고 구하기 
(fire_ts_df.select("CallType")
    .where(col("CallType").isNotNull())
    .groupBy("CallType")
    .count()
    .orderBy("count", ascending=False)
    .show(n=10 , truncate=False))


#min() , max() , sum() , avg()

import pyspark.sql.functions as F 
(fire_ts_df.select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
                  F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins")).show())

