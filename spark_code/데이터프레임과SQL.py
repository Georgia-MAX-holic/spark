from pyspark.sql import SparkSession
#SparkSession 생성 

spark=(SparkSession
      .builder
      .appName("SparkSqLExampleApp")
      .getOrCreate()
       
      )

csv_file ="/databricks-datasets/learning-spark-v2/flights/departuredelay.csv"

#읽고 임시뷰를 생성 
#스키마 추론(더 큰 파일의 경우 스키마를 지정해야함)

df = (spark.read.format("csv")
     .option("inferSchema", "true")
     .option("header", "true")
     .load(csv_file)
     )

df.createOrReplaceTempView("us_delay_flights_tbl")

# ===> 임시뷰 사용 가능, 스파크 sQL을 사용하여 SQL 쿼리 사용 가능 / 비행거리가 1000마리 이상인 모든 항공편 찾기 

spark.sql("""
SELECT distance, origin , destination
FROM us_delay_flights_tbl WHERE distance >1000
ORDER BY distance DESC
""").show(10)

# 샌프란시스코와 시카고 간 2시간 이상 지연이 있었던 모든 항공편 탐색

spark.sql("""
SELECT date, delay, origin, destination
FROM us_delay_flights_tbl
WHERE delay> 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER by delay DESC
""").show(10)


#출발지와 목적지에 관계없이 미국 항공편에 매우 긴 지연 등의 지연에 대한 표시를 레이블로 지정 

spark.sql("""
SELECT delay, origin, destination,
CASE
    WHEN delay > 360 THEN 'Very Long Delays'
    WHEN delay >= 120 AND delay <= 360 THEN 'LONG Delays'
    WHEN delay >= 60 AND delay <120 THEN 'Short Delays'
    WHEN delay > 0 AND delay <60 THEN 'Tolerable Delays'
    WHEN delay = 0 THEN 'No Delays'
    ELSE 'Early'
    
END AS Flight_Delays
FROM us_delay_flights_tbl
ORDER BY origin, delay DESC
""").show(10)


# 얘네 3가지 쿼리는 데이터 프레임 API 쿼리로 표현 가능 

from pyspark.sql.function import col,desc 
(df.select('distance', 'origin', 'destination',)
.where(col('distance')>1000)
.orderBy(desc("distanc"))).show(5)

#또는

(df.select('distance', 'origin', 'destination')
 .where('distance>1000')
 .orderBy('distance', ascending=False).show(10))

########
#######




#SQL 데이터베이스와 테이블 생성하기 

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

#관리형 테이블 생성하기 

spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT , origin STRING, destination STRING)")

#미국 항공편 지연 csv 파일 경로 
csv_file ="databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

#앞의 예제에서 정의된 스키마
schema ="date STRING, delay INT ,distance INT ,origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")



#비관리형 테이블 생성 

spark.sql("""
  CREATE TABLE us_delay_flights_tbl
  (
  
   date STRING,
   delay INT,
   distance INT,
   origin STRING,
   destination STRING
)
USING csv OPTIONS (PATH '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')
"""
)


#데이터 프레임 API에서 

(flights_df
 .write
 .option("path", "/tmp/data/us_flights_delay")
 .saveAstable("us_delay_flights_tbl"))



#뷰 생성하기 / 기존 테이블을 토대로 뷰 만들수 있음 , 일시적이며 스파크 애플리케이션이 종료되면 사라짐 
#SQL 예제

CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_airport_SFO_global_tmp_view AS
    SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE
    origin ="SFO"
    
CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS 
    SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE 
    origin =" JFK"
    
#파이썬 예제 

df_sfo = spark.sql("""
                   SELECT date , delay, origin, destination FROM
                   us_delay_flights_tbl WHERE origin ="sfo"
                   """)

df_jfk = spark.sql("SELECT date, delay , origin, destination FROM us_delay_flights_tbl WHERE origin ='JFK'")

df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")


#DataFrameReader / 데이터 소스에서 프레임으로 데이터를 읽기 위한 핵심 구조 

DataFrameReader.format(args).option("key","value").schema(args).load()

#DataFrameWriter / 지정된 데이터 소스에 데이터를 저장하거나 쓰는 작업을 수행 

DataFrameWriter.format(args)
   .option(args)
    .bucketBy(args)
    .partitionBy(args)
    .save(path)
    
DataFrameWriter.format(args).option(args).sortBy(args).saveTable(table)
