from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("least_Popular_superhero").getOrCreate()

shema = StructType ([\
                     StructField("id", IntegerType(), True),\
                     StructField("hero_name", StringType(), True)])
    
names = spark.read.schema(shema).option("sep", " ").csv("file:///SparkCourse/Marvel-Names.txt")

lines = spark.read.text("file:///SparkCourse/Marvel-Graph.txt")

numb_connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0])\
              .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " "))-1)\
              .groupBy("id").agg(func.sum("connections").alias("connections"))

minConnectionCount = numb_connections.agg(func.min("connections")).first()[0]
               #func.min   - pronadi najmanji broj u stupcu connections
               #first()    - trebala bi biti samo jedna minimalna vrijednost,
               #             npr broj 1 samo, a ne 1 i 2
               #[0]        - da rezultat bude samo 1 stupac, i to taj min(connections)
               
minConnections = numb_connections.filter(func.col("connections") == minConnectionCount)
               # filter(zagrada)  - da je broj connectionsa iz numb_conn df jednak df-u minConnCount

# join dataframes
minConnectionsWithNames = minConnections.join(names, "id")
               # UVIJEK PRVO FILTRIRAJ PRIJE JOINANJA 
               # joinaj dataframe NAMES dataframeu minConnections po ID-ju

print("The following characters have only " + str(minConnectionCount) + " connection(s)")

minConnectionsWithNames.select("hero_name").show()

spark.stop()