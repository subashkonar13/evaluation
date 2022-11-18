import func
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

input_directory = (
            "/opt/application/input"
        )
output_directory = (
                        "/opt/application/output"
        )
spark_session = (
               SparkSession.builder.master("local[*]")
                           .appName("readData")
                           .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                           .getOrCreate()
        )
df=func.readCSV(input_directory,spark_session)
transformedDF=func.transformData(df)
func.writeData(transformedDF,output_directory)
