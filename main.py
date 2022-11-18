from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import concat_ws,col
import pyspark.sql.functions as F
from pyspark.sql.functions import trim
from pyspark.sql import DataFrame
import logging,sys 


def create_logger():

    logger = logging.getLogger("exc_logger")
    logger.setLevel(logging.INFO)
    logfile = logging.FileHandler("exc_logger.log")

    fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(fmt)

    logfile.setFormatter(formatter)
    logger.addHandler(logfile)

    return logger


logger = create_logger()


class ReadData:
    def __init__(self):
        self.spark_session = (
            SparkSession.builder.master("local[*]")
            .appName("readData")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
        )
        self.input_directory = (
            "/opt/application/input"
        )
        self.output_directory = (
                        "/opt/application/output"
        )

    def readCSV(self):
        try:
            logger.info(f"Reading the data present in {self.input_directory} ")
            df = self.spark_session.read.options(
                header="True", inferSchema="True", delimiter=","
            ).csv(self.input_directory)
            return df
        except:
            raise ValueError(f"Exiting as there is no data in {self.input_directory}")

    def transformData(self,df):
        df.cache()

        # Filter values based on TransactionType and derived column flatrate
        listValues = ["Charge", "Charged", "charge", "charged"]
        df = df.withColumn(
            "flatrate",
            when(
                (df["Product Title"].contains("Nursing"))
                & (df["Product Title"].contains("Subscription")),
                "Nursing",
            )
            .when(
                (df["Product Title"].contains("Medicine"))
                & (df["Product Title"].contains("Subscription")),
                "Medicine",
            )
            .when(df["Product Title"].isNull(), "undefined")
            .otherwise(df["Product Title"]),
        )
        df = df.filter((df.flatrate == "Nursing") | (df.flatrate == "Medicine")).filter(
            df["Transaction Type"].isin(listValues)
        )
        df = df.drop(df["Transaction Type"])

        # Renaming column names
        Data_list = [
            "order_no",
            "transaction_date",
            "Transaction_Time",
            "product_title",
            "Currency",
            "amount",
            "flatrate",
        ]
        df = df.toDF(*Data_list)

        # Converting PST TO CET
        df = (
            df.select(
                "order_no",
                "product_title",
                "Currency",
                "amount",
                "flatrate",
                concat_ws(" ", df.transaction_date, df.Transaction_Time).alias(
                    "date_time_pst"
                ),
            )
            .withColumn("new_date_pst", trim(col("date_time_pst")))
            .withColumn(
                "new_date_utc",
                F.to_timestamp("new_date_pst", format="MMM d, yyyy hh:mm:ss a z"),
            )
            .withColumn("date_time", F.from_utc_timestamp("new_date_utc", "CET"))
            .drop(*("new_date_utc", "date_time_pst", "new_date_pst"))
        )

        # Dropping rows with null values
        return df.na.drop("all")

    def writeData(self, df):
        df.cache()
        logger.info("Proceeding with writing to Destination")
        df.coalesce(1).write.partitionBy("flatrate").mode("overwrite").parquet(
            self.output_directory
        )

    

if __name__ == "__main__":
   p = ReadData()
   df = p.readCSV()
   transformedDF = df.transform(p.transformData)
   p.writeData(transformedDF)
   logger.info("Write Complete")
