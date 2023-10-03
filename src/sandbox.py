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
    name = json_data["name"]
    print(f"Name: {name}")
    return {"status": "ok", "request": "req", "response": "res"}


def print_result(row):
    print(f"Row: {row}")


data = [["1", "sravan", "apple", 10],
        ["2", "ojaswi", "google", 20],
        ["3", "bobby", "microsoft", 30],
        ["4", "rohith", "google", 40],
        ["5", "gnanesh", "facebook", 50]]

columns = ['id', 'name', 'company', 'random']

dataframe = spark.createDataFrame(data, columns)
dataframe.filter(dataframe['company'] == 'google').show()
print()

udf_call_pos_api = udf(call_pos_api, udf_pos_schema).asNondeterministic()
dataframe.show()
dataframe = dataframe.select(to_json(struct([dataframe[x] for x in dataframe.columns])).alias("value"))
dataframe = dataframe.withColumn("result", udf_call_pos_api(col("value"))).cache()
dataframe.show()

# x = dataframe.first()
# x.asDict()['result'].asDict()

dataframe.select("result").foreach(lambda result: print_result(to_json(result)))
print()
