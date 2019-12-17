# Databricks notebook source
from pyspark.sql import SQLContext, Row

# COMMAND ----------

from pyspark.sql import SparkSession,Row
import collections


# COMMAND ----------

spark = SparkSession.builder.config("spark.sql.warehouse.dir").appName("SparkSQL").getOrCreate()


# COMMAND ----------

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

# COMMAND ----------

lines = spark.sparkContext.textFile("dbfs:/FileStore/tables/fakefriends.csv")
people = lines.map(mapper)

# COMMAND ----------

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# COMMAND ----------

teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")


# COMMAND ----------

for teen in teenagers.collect():
  print(teen)

# COMMAND ----------

schemaPeople.groupBy("age").count().orderBy("count").show()

# COMMAND ----------

teenagers.groupBy("age").count().max("count").show()

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

#creating datframe from list of tuples 
from pyspark.sql import Row
from pyspark.sql.types import *
l = [('Ankit',25),('Jalfaizy',22),('saurabh',20),('Bala',26)]
rdd = sc.parallelize(l)
persons = rdd.map(lambda x: Row(name = x[0], age = int(x[1])))
schemapersons = sqlContext.createDataFrame(persons)
schemapersons.registerTempTable('schemapersons_table')

# COMMAND ----------


sqlContext.sql("select * from schemapersons_table where age > 22").show()
#type(rdd)
#type(schemapersons)

# COMMAND ----------

rdd.take(4).foreach()

# COMMAND ----------

counter = 0
rdd = sc.parallelize(data)

# Wrong: Don't do this!!
def increment_counter(x):
    global counter
    counter += x
rdd.foreach(increment_counter)

print("Counter value: ", counter)

# COMMAND ----------

#  creating datframe from CSV
import pandas as pd
from pyspark.sql.types import *
csvdataframe = sqlContext.read.format("csv").options(header = True ,inferSchema = True)
display(csvdataframe)

#schema = StructType([StructField("ID", IntegerType(), True),StructField("name", StringType(), True),StructField("age", IntegerType(), True), StructField("numfriends", IntegerType(), True)])
#csvdatframe2 = sqlContext.createDataFrame(pd_df, schema = schema)

# COMMAND ----------

CREATE TABLE fakefriends
  USING csv
  OPTIONS (path "dbfs:/FileStore/tables/fakefriends.csv", header "true", mode "FAILFAST")

# COMMAND ----------

train = sqlContext.load(source="com.databricks.spark.csv", path = 'PATH/train.csv', header = True,inferSchema = True)

# COMMAND ----------

#reading the extrenal text file into the rdd performng the basic transformations

lines = sc.textFile("dbfs:/FileStore/tables/Book.txt")
def clean(l):
  fields = l.split(' ')
  return(fields)
  
pairs = lines.flatMap(lambda x : x.split(" ")).filter(lambda x: len(x) > 3).map(lambda  s : (s, 1))

pairs2 = pairs.reduceByKey(lambda x,y: x+y).collect()


for i in pairs2:
  print(i)
#pairs2 = pairs.mapValues(lambda s: (s, 1))
#pairs2.reduceByKey(lambda x,y: x+y).collect()

# COMMAND ----------

ds.flatMap(x => x.split(" "))

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType([StructField('Id',IntegerType(),True),
                     StructField('Name',StringType(),True),
                     StructField('Age',IntegerType(), True),
                     StructField('Friends',IntegerType(),True)])

df = spark.read.format('csv').option('header','false').schema(schema).load('dbfs:/FileStore/tables/fakefriends.csv')
display(df)


# COMMAND ----------

temptable = ''
df.createOrReplaceTempView('temptable')

# COMMAND ----------

df5 = spark.sql("select * from temptable")
df5.collect()

# COMMAND ----------

df4 = spark.table("temptable")
sorted(df4.collect()) == sorted(df5.collect())

# COMMAND ----------

#udf
spark.udf.register('countoffriends',lambda x: len(x))
spark.sql('select countoffriends(_c2) from temptable').collect()

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import udf
slen = udf(lambda s: len(s), IntegerType())
_ = spark.udf.register("slen",slen)
spark.sql('select slen(_c2) from temptable').collect()

# COMMAND ----------

from pyspark.sql.functions import pandas_udf,PandasUDFType
pandas_udf('integer',PandasUDFType.GROUPED_AGG)
def sum_udf(v):
  return v.sum()



# COMMAND ----------

_= spark.udf.register("sum_udf", sum_udf)


# COMMAND ----------

q = 'select sum_udf(_c3),_c1 from temptable group by _c1'
  spark.sql(q).collect()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temptable 

# COMMAND ----------

PermTable = ''
df.write.format("parquet").saveAsTable('PermTable')


# COMMAND ----------

l = [('Alice','Ram')]

spark.createDataFrame(l,['name','age']).collect()
type(l)

# COMMAND ----------

rdd = sc.parallelize(l)
spark.createDataFrame(rdd,['name','age']).collect()

# COMMAND ----------

from pyspark.sql import Row
Person = Row('name','age')
person = rdd.map(lambda r: Person(*r))
spark.createDataFrame(person).collect()

# COMMAND ----------

from pyspark.sql.types import * 
schema = StructType([
  StructField('name', StringType(),True),
  StructField('age', StringType(),True)
])

df3 = spark.createDataFrame(rdd,schema)
df3.collect()

# COMMAND ----------

df = spark.createDataFrame([("a", 1), ("b", 2), ("c",  3)], ["Col1", "Col2"])
df.select(df.colRegex("`(Col1)?+.+`")).collect()


# COMMAND ----------

df.createGlobalTempView("friends")

# COMMAND ----------

df2 = spark.sql('select * from global_temp.friends')

# COMMAND ----------

sorted(df.collect()) == sorted(df2.collect())

# COMMAND ----------

spark.catalog.dropGlobalTempView("friends")

# COMMAND ----------

df.cube('Name',df.Age).count().orderBy("Name", "Age").collect()

# COMMAND ----------

df.select('Name').distinct().count()

# COMMAND ----------

df.distinct(df.Name).count()

# COMMAND ----------

df.na.drop().show()

# COMMAND ----------

df.filter(df.Age>18).collect()

# COMMAND ----------

df.where(df.Age >18).collect()

# COMMAND ----------

def f(p):
  print(p.Name)
df.foreach(f)

# COMMAND ----------

def f(people):
   for person in people:
      print(person.Name)

# COMMAND ----------

df.foreachPartition(f)

# COMMAND ----------

sorted(df.groupBy('Age').count().head())

# COMMAND ----------

df.isLocal()

# COMMAND ----------

df.isStreaming

# COMMAND ----------

df2 = df.alias('df2')

# COMMAND ----------

df.join(df2, df.Name == df2.Name, 'right_outer').select(df.Name).count()

# COMMAND ----------

cond = [df.Name == df2.Name, df.Age == df2.Age]

# COMMAND ----------

df.join(df2, cond, 'left_anti').select(df.Name).count()

# COMMAND ----------

df.limit(2).show()

# COMMAND ----------

df.sort('Age',ascending = False).collect()

# COMMAND ----------

df.orderBy(['Age','Friends'], ascending= [0,1] ).collect()

# COMMAND ----------

from pyspark.sql.functions import *

df.sort(asc("Age")).collect()

# COMMAND ----------

df.printSchema() 

# COMMAND ----------

df.rdd.collect()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.repartition(2).rdd.getNumPartitions()

# COMMAND ----------

df.rollup('Name',df.Age).count().orderBy('Name','Age').show()

# COMMAND ----------

f = spark.range(10)

# COMMAND ----------

df.sample(fraction = 0.5,seed=3).count()

# COMMAND ----------

df.schema

# COMMAND ----------

df.select(df.Name,(df.Age+10).alias('age')).collect()

# COMMAND ----------

df.show()

# COMMAND ----------

df.toJSON().first()

# COMMAND ----------

 df.toPandas()

# COMMAND ----------

df.withColumn('age2',df.Age + 2).collect()

# COMMAND ----------

df.select('Name', df.time.cast('timestamp')).withWatermark('time', '10 minutes')

# COMMAND ----------

from pyspark.sql.functions import *
df.groupBy('Name').1avg('Age').collect()

# COMMAND ----------

df5= df.repartition(2)

# COMMAND ----------

df5.rdd.getNumPartitions()

# COMMAND ----------

df5 = df5.coalesce(1)

# COMMAND ----------

df5.rdd.getNumPartitions()

# COMMAND ----------

df5.collect()

# COMMAND ----------

type(df.select(df.Age.astype('string')))

# COMMAND ----------

type(df.Age.astype('string'))

# COMMAND ----------

from pyspark.sql import Row 
df9 = spark.createDataFrame([Row(name='Tom', height=80), Row(name='Alice', height=None)])

# COMMAND ----------

df9.filter(df9.height.isNull()).collect()

# COMMAND ----------

from pyspark.sql.functions import *
df.write.format('parquet').bucketBy(100, 'age', 'name').mode('overwrite').saveAsTable('firstBucket')

# COMMAND ----------

df2 = spark.createDataFrame([('ABC',)], ['a'])

# COMMAND ----------

from pyspark.sql.functions import *
df2.select(sha2('a').alias('crc32')).collect()

# COMMAND ----------

dfdt = spark.createDataFrame([('2015-04-08',)], ['dt'])

# COMMAND ----------

dfdt.select(date_format('dt','MM/dd/yyyy').alias('prev_Date')).collect()

# COMMAND ----------

dfdt.select(date_sub(dfdt.dt,1).alias('prev_date')).collect()

# COMMAND ----------

dft = spark.createDataFrame([('1997-02-28 05:02:11',)], ['t'])

# COMMAND ----------

dft.select(date_trunc('quarter',dft.t).alias('month')).collect()

# COMMAND ----------

df = spark.createDataFrame([('2015-04-08','2015-05-10')], ['d1', 'd2'])

# COMMAND ----------

df.select(dayofmonth(df.d1).alias('dif')).collect()

# COMMAND ----------

from pyspark.sql import Row
eDF = spark.createDataFrame([Row(a=1, intlist=[1,2,3], mapfield={"a": "b"})])

# COMMAND ----------

eDF.select(explode(eDF.intlist).alias('anInt')).collect()

# COMMAND ----------

eDF.select(explode(eDF.mapfield).alias('key','value')).show()

# COMMAND ----------

 df9 = spark.createDataFrame([(1, ["foo", "bar"], 1), (2, [], {}), (3, None, None)],("id", "an_array", "a_map"))

# COMMAND ----------

df9.select('id','an_array', explode('a_map')).show()

# COMMAND ----------

df6 = spark.createDataFrame([([[1, 2, 3], [4, 5], [6]],), ([None, [4, 5]],)], ['data'])

# COMMAND ----------


