from datetime import datetime
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
def readCSV(input_directory,spark_session):
        try:
           logger.info(f"Reading the data present in {input_directory} ")
           df = spark_session.read.options(
                header="True", inferSchema="True", delimiter=","
                ).csv(input_directory)
           return df
        except:
            raise ValueError(f"Exiting as there is no data in {input_directory}")