#!/usr/bin/python3
import re
import sys
import logging
import time
from datetime import datetime, timedelta
from hdfs import InsecureClient
from tenacity import retry, wait_fixed, before_sleep_log
import config.aggregatorConfig as cfg
from hdfs import InsecureClient

def get_file_count(_hour, _minute):
    return (2 * cfg.FILES_PER_HALF_HOUR * _hour) + (_minute / 30 + 1) * cfg.FILES_PER_HALF_HOUR


def get_previous_hour_file_names(lastFileName, filesList):
    # substring fileHour
    lastFileHour = lastFileName[0:9]
    halfHourSegment = lastFileName[10:11]

    if halfHourSegment == '30':

        previousFileHourSub = str(int(lastFileHour) - 1)
        newList = get_existing_files(previousFileHourSub, filesList)
        return newList
    else:
        return '00'


def get_existing_files(previousfilehoursub, filesList):
    r = re.compile(previousfilehoursub + ".*")
    client_hdfs = InsecureClient(cfg.hdfs_url)
    #print(client_hdfs)
    newList = list(filter(r.match, filesList))
    print( len(newList))
    if len(newList) == 0:
        filesToProcess = client_hdfs.list(cfg.archivePath.format(cfg.operator))
    newList = list(filter(r.match, filesToProcess))

    if len(newList) < cfg.FILES_PER_HOUR:
        print(len(newList))
        # previousFileHourSub = str(int(previousFileHourSub) - 1)
        time.sleep(120)
        # get the input files again to see if the complete list is available
        filesList = client_hdfs.list(cfg.inputPath.format(cfg.operator))
        get_existing_files(previousfilehoursub, filesList)
    else:
        return newList
