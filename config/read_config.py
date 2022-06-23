from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType
from config.config_function import read_schema

spark = SparkSession.builder.appName("Paint_Cloud_Validation(PCM)").getOrCreate()

# Refer below link or steps for Cloud Storage Connector
# https://kashif-sohail.medium.com/read-files-from-google-cloud-storage-bucket-using-local-pyspark-and-jupyter-notebooks-f8bd43f4b42e
# It is a jar file, Download the Connector and store jar file in spark folder created in C drive.
# the spark has loaded GCS file system and you can read data from GCS.
# You need to provide credentials in order to access your desired bucket.
# spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
#                                      "<path_to_your_credentials_json>")

spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",
                                     r"D:\Google Cloud Data\radiant-saga-351413-457e57ce1bc8.json")

#######################################################################

# Reading the confings

config = ConfigParser()
config.read("config.ini")
fileSchemaFromConfig = config.get('schema','file_schema')
fileSchema = read_schema(fileSchemaFromConfig)

stud_data_df = spark.read.csv(path=r"gs://paint_cloud_migration_bucket/input_files/student data.csv",schema=fileSchema)
stud_data_df.show()


##############################################################

