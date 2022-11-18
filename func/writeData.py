import logging,sys 
from datetime import datetime

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
def writeData(df,output_directory):
        df.cache()
        logger.info("Proceeding with writing to Destination")
        df.coalesce(1).write.partitionBy("flatrate").mode("overwrite").parquet(
            output_directory
        )
