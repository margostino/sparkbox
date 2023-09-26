import json

import pandas as pd
import pyarrow as pa

# dp_sample = pd.read_parquet('/some.parquet', engine='pyarrow')
# # dp_sample = dp_sample[['name', 'last_name', 'age', 'company']]
# dp_sample = dp_sample[['last_name']]
# dp_sample.to_parquet('dp_short_sample.gz', compression='gzip')
# print("done")
# df = pd.DataFrame({'one': ['some', 'dummy', 'data'],
#                    'two': ['foo', 'bar', 'baz'],
#                    'three': ["mock1", "mock2", 'mock3']},
#                   index=list('abc'))


mock_json_value = {
    "metadata": {
        "event_id": "477864ba-6a80-4be1-8f26-4d5800fcecbe"
    },
    "profile": {
        "name": "John",
        "last_name": "Doe",
        "age": 30,
        "company": {
            "name": "Some Inc.",
            "address": "Some Street 123"
        }
    }
}

mock_value = json.dumps(mock_json_value)

df = pd.DataFrame({'id': ["mock_1", "mock_2", "mock_3"],
                   'value': [mock_value, mock_value, mock_value]})

table = pa.Table.from_pandas(df)
# pq.write_table(table, 'sample.parquet')
df.to_parquet('sample.gz', compression='gzip')
