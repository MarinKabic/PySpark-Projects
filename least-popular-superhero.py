from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Creating a Spark Session
spark = SparkSession.builder.appName("least_Popular_superhero").getOrCreate()

# Defining the schema for the dataset
shema = StructType ([\
                     StructField("id", IntegerType(), True),\
                     StructField("hero_name", StringType(), True)])

# Loading the data with ID's and Names
names = spark.read.schema(shema).option("sep", " ").csv("file:///SparkProjects/Marvel-Names.txt")
lines = spark.read.text("file:///SparkProjects/Marvel-Graph.txt")

# Establishing the connections between superheroes
numb_connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0])\
              .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " "))-1)\
              .groupBy("id").agg(func.sum("connections").alias("connections"))

# Determining the minimal number of connections
minConnectionCount = numb_connections.agg(func.min("connections")).first()[0]    

minConnections = numb_connections.filter(func.col("connections") == minConnectionCount)

# Joining the dataframes
minConnectionsWithNames = minConnections.join(names, "id")

# Showing the results
print("The following characters have only " + str(minConnectionCount) + " connection(s)")
minConnectionsWithNames.select("hero_name").show()

# stop the spark session
spark.stop()
