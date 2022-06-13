

class ReadDataUtil:
    # def __init__(self):

    def readCsv(self,spark,path,schema=None,inferschema=True,header=True,sep=","):
       """
Returns new Dataframe by reading provided csv file
       :param spark: spark session
       :param path: csv file path or directory path
       :param schema : provide schema when inferschema is false
       :param inferschema: if True: detect file schema else false : ignore auto detect schema
       :param header: if true input csv file has header
       :param sep : default : "," specify seperator present in csv file
       :return:
       """

       if (inferschema is False)  and (schema == None):
           raise Exception (" Please provide inferschema as true els eprovide schema for given input file")

       if schema == None:
           readdf=spark.read.csv(path=path,inferSchema=inferschema,header=header,sep=sep)
       else:
           readdf=spark.read.csv(path=path,schema=schema,header=header,sep=sep)
       return readdf