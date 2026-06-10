
from pyspark import pipelines as dp

CATALOG = spark.conf.get("CATALOG")
### ---- registrar sink

dp.create_sink(
    name = "write_to_delta_table",
    format= "delta",
    options= {
        "tableName" : f"{CATALOG}.ly_gold.managed_client"
    }
)

### ingestar datos

@dp.append_flow(
    name = "write_to_delta_flow",
    target = "write_to_delta_table"
)

def write_to_delta_flow():
    return spark.readStream.table(f"{CATALOG}.ly_gold.clients_curated")

