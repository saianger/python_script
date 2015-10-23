#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MediaMind_CLD_input.py
#
#   Paramter file for ec2_s3_redshift.py ETL script
#   Copy data into redshift cluster
#
# Assumptions:
#
# 1. You know what you are doing.
# 2. If you don't know what you are doing, you have the sense to ask someone who can assist you.
# 3. If you still don't know, you will LEAVE IT ALONE.
#
# Copyright (c) 2015 Anomaly
# Author Tony Edward
#----------------------------------------
#     I M P O R T A N T  N O T E S
#
#      Read the above Assumptions
#----------------------------------------

import os;

#*# Machine alterable code below this line.

DATEFORMAT1 = 'DD/MM/YYYY HH24:MI:SS';
TIMEFORMAT1 = 'MM/DD/YYYY HH24:MI';

# File System

SRC_DIR = anom.json_obj['Local'].format(anom.JENKINS_ENV['JOB_NAME']);

# set list of input files and s3 paths from json object

FILE_PROC_LIST = [];
S3_PATH = [];

FILES = anom.json_obj['Files'];
S3_TEMPLATE = anom.json_obj['s3']['s3-path'].format(anom.JENKINS_ENV['JOB_NAME'], '{0}');

for i in range(0, len(FILES)):
  FILE_PROC_LIST.append(FILES[i].keys()[0]);                                                                                  # add files
  S3_PATH.append(S3_TEMPLATE.format(FILES[i][FILES[i].keys()[0]]));                                                           # add S3 path to storage (also known as S3 KEY)

# List of redshift tables must correspond with above list of input files

REDSHIFT_TABLE_LIST = [];                                                                                                     # no load to redshift for this project

# PostgreSQL/Redshift

STAGING = 'staging_';
if len(anom.options.schema) < 1:
  SCHEMA_NAME = '';                                                                                                           # hard coded
else:
  SCHEMA_NAME = '%s.' % anom.options.schema.rstrip('.');                                                                      #json overload

#SCHEMA_NAME = '';
ERROR_COUNT = anom.json_obj['MaxError'];
SQL_DEL = ',';

if len(anom.options.dbname) < 1:
  DBNAME = 'dsaf';                                                                                                            # hard coded
else:
  DBNAME = anom.options.dbname;                                                                                               # json overload

# Staging SQL statements (encapsulated in a function that can return a list of dynamically created SQL statements)

#--------------- W A R N I N G -------------------
#       The Magic of Dynamic SQL at work
#
# Any placeholder '%s' that is loaded with a '{0}'
# in the list of string substitution parameters
# will be populated at runtime with the current
# element from REDSHIFT_TABLE_LIST.
# Make sure you understand the mechanism of the
# SQL_LIST list prior to making any changes.
#
# If unsure of the process, ask someone who knows.
# If the Anomaly WIKI is working, consult the WIKI
# for a list of knowledgable boffins who do.
#-------------------------------------------------

def staging_sql():

  SQL_LIST = [];
  return SQL_LIST;

# Email constants

TO_ADDR = ['tony.edward@anom.com.au', 'leo.li@anom.com.au'];

#------------------------------------
# End of parameter script
#------------------------------------
