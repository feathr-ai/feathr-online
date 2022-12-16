######################################################################
# This demo show how to use Piper in PySpark UDF
######################################################################

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField, MapType
from feathrpiper import Piper

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

pipelines = r'''
test_udf(x as string)
| project y=convertCase(x)
;
'''

def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr 

# Use Python UDF in Piper
piper = Piper(pipelines, functions = {"convertCase": convertCase})

# Then use Piper in PySpark UDF, pretty much like Matryoshka doll
#
# `Piper.process` returns ([{"field", value}, ...], error_list)
convertUDF = udf(lambda z: piper.process("test_udf", {"x": z})[0][0]["y"])

# Create DataFrame
columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)

# Using UDF on DataFrame
df.select(col("Seqno"), \
    convertUDF(col("Name")).alias("Name") ) \
.show(truncate=False)

# Using UDF on SQL
spark.udf.register("convertUDF", convertCase)
df.createOrReplaceTempView("NAME_TABLE")
spark.sql(r'''select Seqno, convertUDF(Name) as Name from NAME_TABLE''') \
     .show(truncate=False)
