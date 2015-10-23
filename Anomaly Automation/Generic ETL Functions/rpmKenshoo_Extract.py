#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# rpmKenshoo_Extract.py
#
#   Functions file for ec2_s3_redshift.py ETL script
#
# Assumptions:
#
# 1. You know what you are doing.
# 2. If you don't know what you are doing, you have the sense to ask someone hwo can assist you.
# 3. If you still don't know, you will LEAVE IT ALONE.
#
# Copyright (c) 2015 Anomaly
# Author Tony Edward
#----------------------------------------
#     I M P O R T A N T  N O T E S
#
#      Read the above Assumptions
#----------------------------------------

#*# Machine alterable code below this line.

from datetime import datetime, timedelta;
from dateutil.parser import *;
import glob;
import inspect;                                                                                                               # inspect module for errors
import os;

def extract(f_list, d_list, jobname):

  anom.wrt_log(anom.options, anom.inputs);

# get s3 details

  S3 = anom.json_obj['s3'];
  S3_BUCKET = S3['s3-bucket'][1];
  S3_SOURCE = S3['s3-source'];

# Create S3 connection object

  S3_CONN = anom.boto_connection(credentials=anom.BOTO_CREDENTIALS, api=anom.S3);

# get list of files to copy from S3 source

  if not anom.boto_list(S3_CONN, S3_BUCKET, S3_SOURCE, anom.WILDCARD):
    anom.boto_connection(conn=S3_CONN);                                                                                       # close the connection
    return False;

  for file in anom.lstval:
    if not anom.boto_file(S3_CONN, S3_BUCKET, S3_SOURCE, file, action=anom.BOTO_GET):                                         # get the file
      anom.boto_connection(conn=S3_CONN);                                                                                     # close the connection
      return False;
    else:
      anom.wrt_log("Retrieved file %s from s3://%s/%s" % (file, S3_BUCKET, S3_SOURCE));
      if not anom.boto_file(S3_CONN, S3_BUCKET, S3_SOURCE, file, action=anom.BOTO_DELETE):                                    # delete the source file
        anom.wrt_log("Unable to delete file %s in s3://%s/%s" % (file, S3_BUCKET, S3_SOURCE));
        anom.Missing.append("File %s in s3://%s/%s not deleted" % (file, S3_BUCKET, S3_SOURCE));

# Log missing file types

  anom.Missing.extend(f_list);                                                                                                # initialize build missing list

  for f in f_list:
    for l in anom.lstval:
      if f in l:
        anom.Missing.remove(f);                                                                                               # remove from missing list
        break;

  anom.boto_connection(conn=S3_CONN);                                                                                         # close the connection

  return True;
