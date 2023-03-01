# !usr/bin/python
# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from SparkStart import processCSVFiles
import config.aggregatorConfig as cfg


LOGGER = cfg.get_logger(__name__)

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('Py spark SMS Aggregator')

    while True:
        #client_hdfs = InsecureClient(WEBHDFS_URL)
        spark = cfg.spark
        #initialize the hour to read files from
        fileHourToBeRead = spark.sparkContext.textFile(cfg.dateFile.format(cfg.operator)).first()

        spark.sparkContext.addFile(cfg.dateFile.format(cfg.operator), False)
        processCSVFiles(spark, fileHourToBeRead)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
