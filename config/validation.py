import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from utils.readdatautil import ReadDataUtil

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("Paint_Cloud_Validation(PCM)").getOrCreate()

    rdu = ReadDataUtil()
##########################################################################

 # loading Config File
with open(r"D:\Google Cloud Data\Paint_Cloud_Migration_Files\input_files\stud_info.json") as stud_schema:
    stud_info = json.load(stud_schema)
print(type(stud_info))
print(stud_info)


dic_types = {
    "string":StringType(),
    "integer":IntegerType()
}

# Creating empty schema
sch = StructType()

# Adding column name and datatype in empty schema
for i in stud_info:
    sch.add(i["col_name"],dic_types[i["col_type"]],True)

print(sch)

stud_data_df = rdu.readCsv(spark=spark,path=r"gs://paint_cloud_migration_bucket/input_files/student_data.csv")
print(stud_data_df.schema)

# Matching given schema with file schema for schema validation
if sch == stud_data_df.schema:
    print("Schema is Validated")
else:
    print("Schema is not Validated")

# Validation on Null value
# file_df = stud_data_df.drop().dropna()
#
# if file_df.count() == stud_data_df():
#     print("File has no null value")
# else:
#     print("File has null value")
#
# # Special Character Validation
# a = 0
# for i in stud_data_df.columns:
#     print(i)
#     b = stud_data_df.filter(stud_data_df[i].rlike('[@_!#$^&*()<>?/\|{}~:]')).count()
#     a = a+b
# print(a)
