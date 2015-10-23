#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# SizmekAPI_Input.py
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

########## only for development ##############
# from etl.anomsys import AnomSys
# anom = AnomSys()
########## only for development ##############

#*# Machine alterable code below this line.

DATEFORMAT1 = 'YYYY-MM-DD';
TIMEFORMAT1 = 'MM/DD/YYYY HH24:MI';

# File System

SRC_DIR = '%s/' % os.environ['WORKSPACE'];

# list of input files

FILE_PROC_LIST = anom.json_obj['Files'];

# List of redshift tables must correspond with above list of input files

REDSHIFT_TABLE_LIST = anom.json_obj['Tables'];

# S3 path to storage (also known as redshift KEY)
S3 = anom.json_obj['s3'];
S3_PATH = S3['s3-path'];

# PostgreSQL/Redshift

DBNAME = 'dsaf';
#DBNAME = 'dev';
STAGING = 'staging_';
SCHEMA_NAME = 'xxlq.';
#SCHEMA_NAME = '';
ERROR_COUNT = 3;
SQL_DEL = ',';

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

  SQL_LIST = \
  [
    [
      "BEGIN TRANSACTION;",
      "DELETE FROM %s%s WHERE id IN (SELECT id FROM %s%s%s);" % (SCHEMA_NAME, '{0}', SCHEMA_NAME, STAGING, '{0}'),
      "INSERT INTO %s%s (SELECT * FROM %s%s%s);" % (SCHEMA_NAME, '{0}', SCHEMA_NAME, STAGING, '{0}'),
      "END TRANSACTION;",
    ],
    [
      "BEGIN TRANSACTION;",
      "DELETE FROM %s%s WHERE camid IN (SELECT DISTINCT camid FROM %s%s%s);" % (SCHEMA_NAME, '{0}', SCHEMA_NAME, STAGING, '{0}'),
      "INSERT INTO %s%s (SELECT * FROM %s%s%s);" % (SCHEMA_NAME, '{0}', SCHEMA_NAME, STAGING, '{0}'),
      "END TRANSACTION;",
    ],
    [
      "BEGIN TRANSACTION;",
      "DELETE FROM %s%s WHERE camid IN (SELECT DISTINCT camid FROM %s%s%s);" % (SCHEMA_NAME, '{0}', SCHEMA_NAME, STAGING, '{0}'),
      "INSERT INTO %s%s (SELECT * FROM %s%s%s);" % (SCHEMA_NAME, '{0}', SCHEMA_NAME, STAGING, '{0}'),
      "END TRANSACTION;",
    ],
  ];

  return SQL_LIST;

# Email constants

TO_ADDR = ['tony.edward@anom.com.au', 'leo.li@anom.com.au', 'bernie.kim@anom.com.au'];

#------------------------------------
# End of parameter script
#------------------------------------
