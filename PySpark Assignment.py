# Databricks notebook source
from pyspark.sql import SparkSession 

spark=SparkSession.builder.appName("Selectiva_PySpark_Assignment").getOrCreate()

# COMMAND ----------

# 1. reading csv

# option 1:
# df=spark.read.format("csv").option("inferSchema", "True").option("header","True").option("delimiter",";").load("dbfs:/FileStore/username.csv")


#option 2:

csv_df=spark.read.csv("dbfs:/FileStore/username.csv",inferSchema=True, header=True,sep=';')

csv_df.show()



# COMMAND ----------

# 2. read json:

json_df=spark.read.json("dbfs:/FileStore/books1.json")
json_df.display()


# COMMAND ----------

# 3. reading Parquet

parquet_df=spark.read.parquet("dbfs:/FileStore/userdata1.parquet")
parquet_df.display()


# COMMAND ----------

# 4. reading Avro file

avro_df=spark.read.format("avro").load("dbfs:/FileStore/userdata5.avro")
avro_df.display()

# COMMAND ----------

data1 = [(1, "John", "Sales"),(2, "Jane", "Marketing"),(3, "Bob", "Engineering"),(4, "Alice", "HR"),(5, "Charlie", "Sales")]
column_1=["Id","Name","Department"]
df_1=spark.createDataFrame(data1,column_1)
data2 = [("Sales", "New York"),("Marketing", "San Francisco"),("Engineering", "Seattle"),("HR", "Chicago")]
column_2 = ["Department", "location"]
df_2=spark.createDataFrame(data2,column_2)

# COMMAND ----------

# 5.broadcast join 

from pyspark.sql.functions import broadcast
Join_DF=df_1.join(broadcast(df_2),"Department")      
Join_DF.select("Id","Name","Department").display()

# COMMAND ----------

# 6. Example for Filtering the data 
Join_DF.filter("Id>3").show()
Join_DF.filter(Join_DF.Id>3).show()
Join_DF.filter((Join_DF.Id>3) & (Join_DF.Department=="Sales")).show()

# COMMAND ----------

# 7. aggregate functions 

from pyspark.sql.functions import sum, avg, mean,median, min,max
json_df.groupBy("genre_s").agg(sum(json_df.pages_i).alias("total Pages"),
                                min(json_df.pages_i).alias("Minimum Pages"),
                                max(json_df.pages_i).alias("Max Pages"),
                                median(json_df.price).alias("Median Price")).show()

# COMMAND ----------

#8. Read json file with typed schema (without infering the schema)

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, FloatType

schema = StructType([
    StructField("author", StringType(), True),
    StructField("cat", StringType(), True),
    StructField("genre_s", StringType(), True),
    StructField("id", StringType(), True),
    StructField("inStock", BooleanType(), True),
    StructField("name", StringType(), True),
    StructField("pages_i", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("sequence_i", IntegerType(), True),
    StructField("series_t", StringType(), True)
])

# Read JSON file with defined schema
json_df_with_Schema = spark.read.json("dbfs:/FileStore/books1.json", schema=schema)
json_df_with_Schema.show()

# COMMAND ----------

# 9. Example for increase and decrease number of dataframe partitions 

print("Number of Partitions in df_1 = ",df_1.rdd.getNumPartitions())
df_1=df_1.repartition(3)
print("Number of Partitions in df_1 after repartition = ",df_1.rdd.getNumPartitions())
df_1=df_1.repartition(9)
print("Number of Partitions in df_1 after repartition = ",df_1.rdd.getNumPartitions())


# COMMAND ----------

# 10. Example for renaming the column of the dataframe 

df_1=df_1.withColumnRenamed("Department", "Dep")
df_1.show()

# COMMAND ----------

# 11. Example for adding a new column to the dataframe 
df_1.withColumn("Password",df_1.Id*2343233120).show()

# COMMAND ----------

# 12. Changing the structure of the dataframe 

dataa = [
    """{ "name" : "john doe", "dob" : "01-01-1980" }""",
    """{ "name" : "john adam", "dob" : "01-01-1960", "phone" : 1234567890 }""",
    """{ "name" : "jack", "dob" : "15-05-1990", "email" : "jane@example.com" }""",
    """{ "name" : "mike", "dob" : "30-11-1985", "phone" : 9876543210, "email" : "mike@example.com" }""",
    """{ "name" : "sarah", "dob" : "22-07-1975" }"""
] 

rdd = sc.parallelize(dataa)
df_json = spark.read.json(rdd)
df_json.show()


# COMMAND ----------

from pyspark.sql.functions import from_json, struct, to_json

structured_df = df_json.withColumn("personal_data", struct(df_json.columns)).select("personal_data")

structured_df.write.mode("overwrite").json("dbfs:/FileStore/output1.json")



# COMMAND ----------

spark.read.json("dbfs:/FileStore/output1.json").display()

# COMMAND ----------


