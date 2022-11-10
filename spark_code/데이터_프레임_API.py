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
