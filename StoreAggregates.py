from pyspark.sql.functions import count, sum, lit, when, col, udf, coalesce
import config.aggregatorConfig as cfg

def aggregateCallsForTheday(spark, date):
    print('Daily Aggregate saves' +':::'+'hourly_sms_counts_'+cfg.operator)
    callsCollection = spark.read.format('mongo').option('database', 'hourly_cdr_counts_'+cfg.operator). \
        option('collection', 'outgoing_counts').load()
    callsCollection.printSchema()


    cc1 = callsCollection.filter(callsCollection["CALL_DATE_HOUR"].rlike(date[0:8]))
    cc2 = cc1.groupBy(callsCollection["MSISDN"]).agg(sum(callsCollection["num_out"]).alias("num_outs"),
                                                         sum(callsCollection["num_in"]).alias("num_ins"),
                                                         sum(callsCollection["tot_out_duration"]).alias("tot_outs"),
                                                         sum(callsCollection["tot_in_duration"]).alias(
                                                             "tot_ins")).withColumn("CALL_DATE",
                                                                                    lit(date))

