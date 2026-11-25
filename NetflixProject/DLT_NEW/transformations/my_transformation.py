import dlt
from pyspark.sql.functions import *





@dlt.table(
    name = "gold_netflix_directories"
)
def myfunction():
    df = spark.readStream.table("netflix.bronze.netflix_directors")
    return df


