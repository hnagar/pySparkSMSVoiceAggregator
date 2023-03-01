import time
from _datetime import datetime, timedelta

from hdfs import InsecureClient
from pyspark.sql.functions import count, sum, lit
from pyspark.sql.types import IntegerType, StructType, StructField, StringType



import config.aggregatorConfig as cfg
from GetCSVFilesForTheHour import get_existing_files
from StoreAggregates import aggregateCallsForTheday

fileSchema = StructType([StructField('CALL_DATE_HOUR', StringType(), True),
                                 StructField('SW_ID', StringType(), True),
                                 StructField('S_IMSI', StringType(), True),
                                 StructField('S_IMEI', StringType(), True),
                                 StructField('S_CI', StringType(), True),
                                 StructField('S_LAC', StringType(), True),
                                 StructField('TRUNK_IN', StringType(), True),
                                 StructField('TRUNK_OUT', StringType(), True),
                                 StructField('TERM_CAUSE', StringType(), True),
                                 StructField('TERM_REASON', StringType(), True),
                                 StructField('SS_CODE', StringType(), True),
                                 StructField('CHARGE_INDICATOR', StringType(), True),
                                 StructField('CALL_ID', StringType(), True),
                                 StructField('O_MSISDN', StringType(), True),
                                 StructField('CALL_START_TIME', StringType(), True),
                                 StructField('DURATION', IntegerType(), True),
                                 StructField('O_CALL_ID', StringType(), True),
                                 StructField('CALL_TYPE', StringType(), True),
                                 StructField('CALL_END_TIME', StringType(), True),
                                 StructField('S_MSISDN', StringType(), True)
                                 ])

def processCSVFiles(spark, fileHourToBeRead):
    # kill previous executors
    sc = spark._jsc.sc()
    #Create fileSystem object using the hadoop config
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())

    n_workers = len([executor.host() for executor in sc.statusTracker().getExecutorInfos()]) - 1
    print("workers are :" + str(n_workers))
    logger = cfg.get_logger("voiceAggregator")
    logger.info("The processing Hour is:" + fileHourToBeRead)

    client_hdfs = InsecureClient(cfg.hdfs_url)
    logger.info("The client input path is:"+cfg.inputPath.format(cfg.operator))

    inputfiles =client_hdfs.list(cfg.inputPath.format(cfg.operator))
    logger.info("The list of input files:{}".format(inputfiles))

    INPUT_PATH = cfg.inputPath.format(cfg.operator)
    ARCHIVE_PATH = cfg.archivePath.format(cfg.operator)
    # df = spark.read.format("csv").load("hdfs://apollo01-hb01/user/hadoop/callsLoader_615_01/202208160830_00.csv")
    # df.show(5)

    if len(inputfiles) > 0:
        #print(inputFiles[inputFiles[len(inputFiles) - 1]])
        filesToProcess = get_existing_files(fileHourToBeRead, inputfiles)
        print(filesToProcess)
        hourToProcess = filesToProcess[0][0:10]
        print(hourToProcess)
        #
        dfUnion = spark.createDataFrame(spark.sparkContext.emptyRDD(), fileSchema)

        for file in filesToProcess:
            print(cfg.hdfs+INPUT_PATH + file)
            #df = spark.read.load(cfg.hdfs + INPUT_PATH + file, format='csv', schema=fileSchema)
            #df = spark.read.load("hdfs://apollo01-hb01/" + INPUT_PATH + file, format='csv', schema=fileSchema)
            # calls[DURATION] = calls[DURATION].astype(int)
            if fs.exists(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(cfg.hdfs + INPUT_PATH + file)):
                df = spark.read.load(cfg.hdfs + INPUT_PATH + file, format='csv', schema=fileSchema)
            else:
                print("File is in archive")
                df = spark.read.load(cfg.hdfs + ARCHIVE_PATH + file, format='csv', schema=fileSchema)
            dfUnion = dfUnion.union(df)
            df.printSchema()

        outGoingCalls = dfUnion.filter(dfUnion.CALL_TYPE.isin(cfg.MOC_CALL_TYPES))
        inComingCalls = dfUnion.filter(dfUnion.CALL_TYPE.isin(cfg.MTC_CALL_TYPES))
        outGoingAggs = outGoingCalls.groupby(dfUnion.S_MSISDN).agg(count(dfUnion.S_MSISDN).alias('num_out'))
        inComingAggs = inComingCalls.groupby(dfUnion.S_MSISDN).agg(count(dfUnion.S_MSISDN).alias('num_in'))

            # print(outGoingAggs.head(20))
            # print(inComingAggs.head(20))

        #aggs = outGoingAggs.join(inComingAggs, outGoingAggs.S_MSISDN == inComingAggs.S_MSISDN, 'outer')
        aggs = outGoingAggs.join(inComingAggs, "S_MSISDN", 'outer')
        aggs.na.fill(value=0)

        aggsNew = aggs.withColumn("CALL_DATE_HOUR", lit(hourToProcess)).withColumnRenamed("S_MSISDN", "MSISDN")

        aggsNew.repartition(2)
        aggsNew.printSchema()

        # store in Mongo
        #aggsNew.write.format('mongo').mode('append').option('database', 'hourly_cdr_counts_'+cfg.operator).\
            #option("collection", "outgoing_counts").partitionBy('MSISDN').save()
        # # If hour 23 is done, create and add to the daily aggregate table for the day.
        #print("The hourToProcess is :" + hourToProcess + "::" + hourToProcess[8:10])
        if (hourToProcess[8:10] == '23'):
            aggregateCallsForTheday(spark, hourToProcess[0:8])



        #Add the file hour to the startDateHOur in the file
        print(hourToProcess)
        date_time_str = hourToProcess
        date_time_obj = datetime.strptime(date_time_str, '%Y%m%d%H')+timedelta(hours=1)
        print(date_time_obj.strftime('%Y%m%d%H'))

        client_hdfs.delete(cfg.dateFilePathForSpark.format(cfg.operator))
        client_hdfs.write(cfg.dateFilePathForSpark.format(cfg.operator), date_time_obj.strftime('%Y%m%d%H'))
            #client_hdfs.write(cfg.datFile.format(cfg.operator), nextHourToProcess, True)
            #return hourToProcess
    else :
        #sleep 30 mins
        time.sleep(30*60)
