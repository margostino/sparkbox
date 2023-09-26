import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType

json_list = []
JSON_STRING = "[1, 2, 3]"
json_list.append(json.loads(JSON_STRING))
x = json.dumps(json_list)
print()

spark = SparkSession.builder.appName('sparkdf').getOrCreate()

udf_pos_schema = StructType([
    StructField("status", StringType()),
    StructField("request", StringType()),
    StructField("response", StringType())
])


def call_pos_api(data):
    json_data = json.loads(data)
    name = json_data["Employee NAME"]
    print(f"SARLANGA: {name}")
    return {"status": "ok", "request": "req", "response": "res"}


data = [["1", "sravan", "company 1", 10],
        ["2", "ojaswi", "company 2", 20],
        ["3", "bobby", "company 3", 30],
        ["4", "rohith", "company 2", 40],
        ["5", "gnanesh", "company 1", 50]]

columns = ['Employee ID', 'Employee NAME',
           'Company Name', 'Random Number']

dataframe = spark.createDataFrame(data, columns)
udf_call_pos_api = udf(call_pos_api, udf_pos_schema).asNondeterministic()
dataframe.show()
dataframe = dataframe.select(to_json(struct([dataframe[x] for x in dataframe.columns])).alias("value"))
dataframe = dataframe.withColumn("result", udf_call_pos_api(col("value"))).cache()
dataframe.show()
print()
