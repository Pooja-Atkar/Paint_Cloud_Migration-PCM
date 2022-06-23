
class ReadDataUtil:
    # def __init__(self):

    def readCsv(self,spark,path,schema=None,inferschema=True,header=True,sep=","):
        """
        Returns new dataframe by reading provided csv file
        :param spark: spark session
        :param path: csv file path or directory path
        :param schema: provide schema,required when inferschema is false
        :param inferschema: if true:detect file schema else false:ignore auto detect schema
        :param header: if true:input csv file has header
        :param sep: default: "," specify separator present in csv file
        :return:
        """
        if (inferschema == False) and (schema == None):
            raise Exception("Please provide inferschema as true else provide schema for given input file")

        if schema == None:
            readdf = spark.read.csv(path=path, inferSchema=True, header=header, sep=sep)
        else:
            readdf = spark.read.csv(path=path,schema= schema,header=header,sep=sep)
        return readdf

    def readParquet(self,spark,path,schema=None):
        if schema==None:
            readpdf = spark.read.parquet(path)
        else:
            readpdf = spark.read.schema(schema).option("mergeSchema","true").parquet(path)
        return readpdf





