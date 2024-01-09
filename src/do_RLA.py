from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DateType
from pyspark.sql.types import StringType, MapType
from pyspark.sql.functions import monotonically_increasing_id 


spark = SparkSession.builder.appName("Datacamp Pyspark Tutorial").config("spark.memory.offHeap.enabled","true").config("spark.memory.offHeap.size","10g").getOrCreate()


BLOCKING_ATTRIBUTE = 1

dict_blocks = {}

col_names = ["u_id","last_name","first_name","dob","dod"]

schema = StructType([
                    StructField('id', IntegerType(), True),
                    StructField('last_name', StringType(), True),
                    StructField('first_name', StringType(), True),
                    StructField('dod', StringType(), True),
                    StructField('dob', StringType(), True)
                ])



def generate_k_mer(str_d, k:int):
    if len(str_d) <= k:
        return [str_d]

    return [str_d[i:i+k] for i in range(0, len(str_d)-(k-1))]

def de_duplication():

    df_ds_1 = spark.read.csv('/home/nachiket/RLA_CL_EXTRACT/data/ds1.1.1',sep ='\t',header=False,schema=schema,escape="\"")
    df_ds_2 = spark.read.csv('/home/nachiket/RLA_CL_EXTRACT/data/ds1.1.2',sep ='\t',header=False,schema=schema,escape="\"")


    df_X = df_ds_1.union(df_ds_2)
# df_X.withColumnRenamed()
#df_X.show(5,0)

    df_X.sort(["last_name","first_name","dod","dob"],ascending = [True]).show(5,0) 

    df_X_prime = df_X.dropDuplicates(["last_name","first_name","dod","dob"])
    
    df_X_prime = df_X_prime.select("*").withColumn("id", monotonically_increasing_id())

    return df_X_prime

def blocking(df_X_prime):
    for rows in df_X_prime.select("id","last_name").collect():
        k_mer_list = generate_k_mer(str(rows[1]),3)

        for asc in k_mer_list:
            if asc.lower() in dict_blocks:
                dict_blocks[asc.lower()].append(rows[0])
            else:
                dict_blocks[asc.lower()] = [rows[0]]
    

