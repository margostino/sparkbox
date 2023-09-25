from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName('sparkdf').getOrCreate()

udf_pos_schema = StructType([
    StructField("status", StringType()),
    StructField("request", StringType()),
    StructField("response", StringType())
])


def call_pos_api(data):
    print(f"SARLANGA: {data}")
    return {"status": "ok", "request": "req", "response": "res"}


# list  of employee data with 5 row values
data = [["1", "sravan", "company 1"],
        ["2", "ojaswi", "company 2"],
        ["3", "bobby", "company 3"],
        ["4", "rohith", "company 2"],
        ["5", "gnanesh", "company 1"]]

# specify column names
columns = ['Employee ID', 'Employee NAME',
           'Company Name']

# creating a dataframe from the lists of data
dataframe = spark.createDataFrame(data, columns)
udf_call_pos_api = udf(call_pos_api, udf_pos_schema).asNondeterministic()
dataframe.show()
dataframe = dataframe.withColumn("result", udf_call_pos_api(col("Employee NAME"))).cache()
dataframe.show()

print()
