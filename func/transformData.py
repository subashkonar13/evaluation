from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import concat_ws,col
import pyspark.sql.functions as F
from pyspark.sql.functions import trim
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

def transformData(df):
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