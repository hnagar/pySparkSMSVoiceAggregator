import logging
from logging.handlers import TimedRotatingFileHandler

from hdfs import InsecureClient
from pyspark.sql import SparkSession

operator = '510_1'
FILES_PER_HALF_HOUR = 1
FILES_PER_HOUR=1
inputPath = '/user/hadoop/callsLoader_{}/'
archivePath = '/user/hadoop/callsLoader_{}_archive/'
dateFile = 'hdfs://mercury01-hb01/opt/dates/smsProfilingDateFile_{}.txt'
hdfs_url = 'http://mercury01-hb01:50070'
hdfs = 'hdfs://mercury01-hb01/'
dateFilePathForSpark = '/opt/dates/smsProfilingDateFile_{}.txt'
hdfscmd='/opt/hadoop/hadoop/bin/hdfs'

client_hdfs = InsecureClient(hdfs_url)
MOC_CALL_TYPES = ['30']
MTC_CALL_TYPES = ['31']

spark = SparkSession.builder.appName("SMSCallsAggregationPhoenix").getOrCreate()

# Logger configuration
file_handler = TimedRotatingFileHandler('/opt/voiceAggregator/{}/logs/Aggregator.log'.format(operator),
                                        when='midnight', backupCount=7)
file_handler.setFormatter(logging.Formatter('%(asctime)s — %(name)s — %(levelname)s — %(message)s'))


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger
