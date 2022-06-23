# All necessary functions

from pyspark.sql.types import *
def read_schema(schema_args):
    dict_types = {
        "StringType()":StringType(),
        "IntegerType()":IntegerType()
    }

    split_values = schema_args.split(",")
    sch = StructType()

    for i in split_values:
        x = i.split(" ")
        sch.add(x[0],dict_types[x[1]],True)
    return sch