#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# DFA_Input.py
#
#   Paramter file for ec2_s3_redshift.py ETL script
#   Copy data into redshift cluster
# 
# Assumptions:
#
# 1. You know what you are doing.
# 2. If you don't know what you are doing, you have the sense to ask someone hwo can assist you.
# 3. If you still don't know, you will LEAVE IT ALONE.
# 
# Copyright (c) 2015 Anomaly
# Author Leo Li
#----------------------------------------
#     I M P O R T A N T  N O T E S
#
#      Read the above Assumptions
#----------------------------------------
import psycopg2;
import os;
#*# Machine alterable code below this line.
from datetime import datetime; 
from datetime import timedelta;


DATEFORMAT1 = 'auto';
TIMEFORMAT1 = 'MM/DD/YYYY HH24:MI';
anom.curdat()
FILE_TIMESTAMP = (anom.datval - timedelta(days=1)).strftime("%m-%d-%Y");
FILE_TIMESTAMP = "05-28-2015"
SRC_DIR = '%s/' % os.environ['WORKSPACE'];
#SRC_DIR = '/mnt/data/jenkins/jobs/DFA_ETL/workspace/'

# list of input files

NETWORK_FILE_LIST = \
[
  'NetworkImpression_7921_' + FILE_TIMESTAMP + '.log',
  'NetworkClick_7921_' + FILE_TIMESTAMP + '.log',
  'NetworkActivity_7921_*' + FILE_TIMESTAMP + '.log',
];


MATCH_FILE_LIST = \
[
];


FILE_DIR_LIST = \
[  
 'impression_training_logs/',
  'click_training_logs/',
  'activity_training_logs/',
];


FILE_PROC_LIST = NETWORK_FILE_LIST + MATCH_FILE_LIST;

# List of redshift tables must correspond with above list of input files

TRANS_TABLE_LIST = \
[
  'dfa_networkimpression',
  'dfa_networkclick',
  'dfa_networkactivity',
]

MATCH_TABLE_LIST = \
[
]


REDSHIFT_TABLE_LIST = TRANS_TABLE_LIST + MATCH_TABLE_LIST

# S3 path to storage (also known as redshift KEY)

S3_PATH = '/data-processing/DFA_download/';

# PostgreSQL/Redshift
DBNAME='dsaf'
STAGING = 'staging_';
SCHEMA_NAME = 'xxlq.';
ERROR_COUNT = 30;
SQL_DEL = '\xFE';

# Staging SQL statements (encapsulated in a function that can return a list of dynamically created SQL statements)

#--------------- W A R N I N G -------------------
#       The Magic of Dynamic SQL at work
#
# Any placeholder '%s' that is loaded with a '%s'
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






# db configuration definition
configuration = { 'dbname': 'dsaf',
                  'user':'anompublic',
                  'pwd':'Duch+thup*629AY',
                  'host':'mbau-ausanom-redshift.c3nkjwrx2zc4.ap-southeast-2.redshift.amazonaws.com',
                  'port':'5439'
                }

# create connection to db
def create_conn(*args,**kwargs):
  config = kwargs['config']
  try:
    conn=psycopg2.connect(dbname=config['dbname'], host=config['host'], port=config['port'], user=config['user'], password=config['pwd'])
  except Exception as err:
    print err.code, err
  return conn


pk_sql = """SELECT c.nspname,b.relname AS TABLE_NAME,
                        d.attname AS COLUMN_NAME
                        FROM pg_catalog.pg_constraint a
                        JOIN pg_catalog.pg_class b
                        ON(a.conrelid=b.oid)
                        JOIN pg_catalog.pg_namespace c
                        ON(a.connamespace=c.oid)
                        JOIN pg_catalog.pg_attribute d
                        ON(d.attnum = ANY(a.conkey) AND a.conrelid=d.attrelid)
                        WHERE a.contype='p' and nspname='%s' and TABLE_NAME='%s'""" % (SCHEMA_NAME.rstrip('.'),'%s')


def build_sql_string(DEST_TABLE):
# build where clause in case there are mutiple primary keys
  where_str=""
  conn = create_conn(config=configuration)
  SOUR_TABLE = STAGING + DEST_TABLE
  with conn:
    with conn.cursor() as cursor:
      try:
        cursor.execute(pk_sql % (DEST_TABLE))
      except psycopg2.Error as e:
        print e.pgerror
      rows = cursor.fetchall()
      i=0
      for row in rows:
        if i == 0:
          where_str += SCHEMA_NAME + '%s' + "." + row[2] + "=" + SCHEMA_NAME + STAGING + '%s' + "." + row[2]
        else:
          where_str += " and " + SCHEMA_NAME + '%s' + "." + row[2] + "=" + SCHEMA_NAME + STAGING + '%s' + "." + row[2]
        i += 1
  return where_str



def staging_sql():
  
  SQL_LIST = \
  [
    [
     "BEGIN TRANSACTION;",
     "INSERT INTO %s%s SELECT TRUNC(starttime),* " % (SCHEMA_NAME, '%s') +\
     "FROM %s%s%s WHERE advertiserid IN (%s);" % (SCHEMA_NAME, STAGING, '%s',','.join(anom.input_lists[0])),
     "END TRANSACTION;"
    ],
  ] * len(NETWORK_FILE_LIST);
  
  for t in MATCH_TABLE_LIST:
    SQL_LIST_SMALL=["BEGIN TRANSACTION;",]
    sql_string = "DELETE FROM %s%s " % (SCHEMA_NAME, '%s') +\
    "USING %s%s%s WHERE " %  (SCHEMA_NAME, STAGING, '%s') + build_sql_string(t)
    SQL_LIST_SMALL.append(sql_string)
    SQL_LIST_SMALL.append("INSERT INTO %s%s SELECT * FROM %s%s%s;" % (SCHEMA_NAME,'%s',SCHEMA_NAME,STAGING,'%s'))
    SQL_LIST_SMALL.append("END TRANSACTION;")
    #SQL_LIST.append(SQL_LIST_SMALL)

  return SQL_LIST;


TO_ADDR = ['leo.li@anom.com.au',''];

#------------------------------------
# End of parameter script
#------------------------------------
