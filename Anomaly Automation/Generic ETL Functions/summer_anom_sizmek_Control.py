#!/bin/python
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ec2_s3_redshift.py
#
#   Put data files from EC2 to S3
#   Copy data into redshift cluster
#
# Assumption:
#
# 1. Parameter file is passed as 1st argument when called by Jenkins
# 2. Parameter file is in Python format assigning input values to variables
#
# This script is to be run by Jenkins, once completed, the data processing
# job(s) are kicked off by Jenkins automatically.
# Copyright (c) 2015 Anomaly
# Author Tony Edward
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# I M P O R T A N T   N O T E S
#
# All SQL statements are executed consecutively by separate calls to mitigate
# legacy issues with 'psql' not necessarily providing consistant results with multiple SQL statements
# Redhat has early 4.5 Crypto and Python2.6 is built with 5 Crypto hence Crypto import and filtering on warnings for pysftp
# This script will not run unless a parameter file that is correctly formatted has been provides as a command line argument
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Redhat old Crypto warning bypass
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

from Crypto.pct_warnings import PowmInsecureWarning;
import warnings;
warnings.simplefilter("ignore", PowmInsecureWarning);

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Python imports
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

import sys;
import os.path;
import os;
import glob;
import json;
import inspect;                                                                                                               # inspect module for errors
import __builtin__;
import AnomSys;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Set global parameters
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Library Declaration and Construction

__builtin__.anom = AnomSys.AnomSys();


# Date/Time

DATEFORMAT1 = 'auto';                                                                                                         # set by command line module import
TIMEFORMAT1 = 'auto';                                                                                                         # set by command line module import

# File System

SRC_DIR = '';                                                                                                                 # set by command line module import
S3_BUCKET = 's3://mbau-syd-cadreon';                                                                                          # S3 bucket name
S3_PATH = '';                                                                                                                 # set by command line module import

# Input files list

FILE_PROC_LIST = [];                                                                                                          # set by command line module import
FILE_DIR_LIST = [];                                                                                                           # set by command line module import

# Input file lists

INPUT_FILES  = [];
S3_GZ_FILES = [];

# Database Tables

STAGING = '';                                                                                                                 # set by command line module import

# Redshift tabel names list

REDSHIFT_TABLE_LIST = [];                                                                                                     # set by command line module import

# PostgreSQL/Redshift

SCHEMA_NAME = '';                                                                                                             # set by command line module import
ERROR_COUNT = '';                                                                                                             # set by command line module import
DBNAME = '';
RS_HOST = '--host mbau-ausanom-redshift.c3nkjwrx2zc4.ap-southeast-2.redshift.amazonaws.com';
RS_PORT = '--port 5439';
RS_USER = '--user anompublic';
RS_DBNAME = '--dbname %s';
SQL_DEL = ',';                                                                                                                # set by command line module import

# Email constants

FR_ADDR = 'anomaly.AWS@mnetmobile.com';
TO_ADDR = [];                                                                                                                 # set by command line module import
SMTP_SVR = 'smtp.gmail.com';
SMTP_PRT = 587;
SMTP_USER = 'anom.com.au@gmail.com';
SMTP_PWD = 'Dailyanom';
SMTP_SBJ = '';                                                                                                                # set from Jenkins job name environment name

# Environment variables

# Access to S3

ACCESS_KEY_CMD = "grep access_key ~/.s3cfg | awk '{print $3}'";                                                               # S3 Access key command
ACCESS_KEY_ENV = 'ACCESS_KEY';                                                                                                # S3 Access key environment name
ACCESS_KEY = '';                                                                                                              # S3 Access key environment variable
SECRET_KEY_CMD = "grep secret_key ~/.s3cfg | awk '{print $3}'";                                                               # S3 Secret key
SECRET_KEY_ENV = 'SECRET_KEY';                                                                                                # S3 Secret key environment name
SECRET_KEY = '';                                                                                                              # S3 Secret key environment variable
JOB_NAME = 'JOB_NAME';                                                                                                        # Jenkins job name environment name
MAGIC = '--no-mime-magic';                                                                                                    # s3cmd command line directive

EXTRACT_FUNC_LIST = [];                                                                                                       # initialise extract function list

# Security considerations

OBFUSCATE_KEYS = [];                                                                                                          # initialise obfuscation key list

# Is Jenkins running this script?

env = os.environ;                                                                                                             # get current O/S environment

if (JOB_NAME in env) and (len(env[JOB_NAME]) > 0):
  anom.Jenkins = True;
else:
  sys.exit("No JOB_NAME, This script is designed to be run from Jenkins, Exiting");

for key in env:
  if (anom.JENKINS_ENV.has_key(key)):
    anom.JENKINS_ENV[key] = env[key];                                                                                              # get jenkins environment

SMTP_SBJ = '%s WARNING' % anom.JENKINS_ENV[JOB_NAME];                                                                              # set email subject

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Command line imports for runtime parameters, dynamic extract, and transform functions
#                    ---------I M P O R T A N T  N O T E---------
# The importable functions can be contained within the runtime parameters script and can
# be over-ridden if necessary by additional scripts imported based on the values in sys.argv.
# This allows for special case extracts and teransforms to be performed without the need to
# alter the productionised code.
# All that is required is to add command line parameters to the Jenkins scheduler or whichever
# scheduler is currnetly being utilised.
#
# Explicit command line options eg: <--database dsaf> and <--schema xxsony.> will have precedence over all other methods.
#
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Overload functions that will be replaced if necessary by the script imports below

def transform(tf, i):

# Check for json format

  with open(tf, 'rb') as f:
    try:
      data = f.readline();
      json.loads(data);
      anom.Errmsg = '%s contains JSON format. needs to be transformed prior to loading into Redshift' % file;
      f.close();
      return False;
    except ValueError as e:
      anom.do_nothing('Transform for %s' % tf);

  return True;

def extract(f, d_list=[], jobname=""):

  anom.do_nothing('Extract for %s' % jobname);
  return True;

RUN_ERROR = \
  'FATAL ERROR\n\n' + \
  'No parameter file specified\n\n' + \
  'USAGE: python ./ETL_control.py <inputfile> [<funcfile>] [options ...]\n\n' + \
  '\t<inputfile> contains parameters for datasources and redshift tables\n' + \
  '\t[<funcfile>] contains optional transformn functions\n' + \
  '\t[<funcfile>] contains optional extract functions\n' + \
  '\t[--start-date <YYYY-MM-DD>] contains optional start date\n' + \
  '\t[--end-date <YYYY-MM-DD>] contains optional end date\n' + \
  '\t[--metrics <metric1,metric2,...>] optional metrics parameters\n' + \
  '\t[--dimensions <dimension1,dimension2,...>] optional dimensions parameters\n' + \
  '\t[--start-index <value>] optional start index defaults to 1\n' + \
  '\t[--max-results <value>] optional maximum results set size defaults to 10\n' + \
  '\t[--filters <filter1=xx,...>] optional filters defaults to country==AU\n' + \
  '\t[--sort <sort field>] optional sort order defaults to -views\n' + \
  '\t[--setup <True or False>] configuration option defaults to False\n' + \
  '\t[--database <name>] optional database name defaults to ""\n' + \
  '\t[--schema <name>] optional schema name defaults to ""\n' + \
  '\t[--debug <name>] optional debug flag defaults to False\n' + \
  '\t[--timer <seconds>] optional Countdown Timer in Seconds defaults to 0)\n' + \
  '\t[--input-lists <[[list1],[list2],...]>] optional Lists of Parameters defaults to "[[]]"\n' + \
  '\t[--json-file <filename>] optional JSON input file for complex Parameters defaults to ""\n\n' + \
  'If unsure of process, Check with Anomaly Developers\n';

# Get command line and store options

if len(anom.inputs) < 1: sys.exit(RUN_ERROR);

# Import all command line arguments

for i in range(0, len(anom.inputs)):
  try:
    exec 'from %s import *' % anom.inputs[i].rstrip(anom.PYTEXT);                                                             # import parameters file
  except Exception as e:
    sys.exit('%s: Please fix prior to running this process' % str(e));                                                        # exit if list lengths are not the same

# Check imported lists conform and are of equal length

if (not len(FILE_PROC_LIST) == len(REDSHIFT_TABLE_LIST)):
  sys.exit('Input File list lengths are not the same. Please fix prior to re-running this process');                          # exit if list lengths are not the same

# Overload command line options if set

if len(anom.options.dbname) < 1:
  pass;
else:
  DBNAME = anom.options.dbname;

if len(DBNAME) < 1: sys.exit('No Database defined: Please fix prior to re-running this process');                             # exit if list lengths are not the same

if len(anom.options.schema) < 1:
  pass;
else:
  SCHEMA_NAME = '%s.' % anom.options.schema.rstrip('.');

# Set psql arguments once input file is read

PSQLARGS = '%s %s %s %s' % (RS_HOST, RS_PORT, RS_USER, RS_DBNAME % DBNAME);

TABLE_STRUCT = None;
if anom.json_obj.has_key('TableStruct'): TABLE_STRUCT = anom.json_obj['TableStruct'];
TRUNCATE = '';
if anom.json_obj.has_key('Truncate'): TRUNCATE = anom.json_obj['Truncate'];                                                   # space at end of strring is important

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Main Program
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

def main():

# set directory

  anom.Orgdir = os.getcwd();                                                                                                  # save current directory

  try:
    os.chdir(SRC_DIR);                                                                                                        # change to sftp directory
    anom.Curpth = SRC_DIR;
  except:
    sys.exit('Cannot change directory to %s' % SRC_DIR);                                                                      # direct exit as no logfile yet

# Delete previous log files

  OLD_LOG = glob.glob('%s%s%s' % (anom.JENKINS_ENV[JOB_NAME], anom.WILDCARD, anom.LOGEXT));                                   # get list of log files
  anom.zip_file(OLD_LOG, '%s_LOG_ARCHIVE%s' % (anom.JENKINS_ENV[JOB_NAME], anom.ZIPEXT), mode='a');                           # zip to log archive
  logflg = anom.del_fil(OLD_LOG, logging=False);                                                                              # delete previous log file(s) ignore if unable

# Create log file

  if (not anom.crt_log(anom.JENKINS_ENV[JOB_NAME])):
    anom.err_exit(anom.Errmsg);

  Delmsg = 'Deleted';
  if (not logflg): Delmsg = 'Unable to delete';
  if (not anom.wrt_log('%s %s ' % (Delmsg, OLD_LOG))):
    anom.err_exit(anom.Errmsg);

# write command line parameters to log file

  anom.wrt_log('%s' % sys.argv);
  anom.wrt_log('Current Directory is %s' % SRC_DIR);
  if (anom.Debug == True): anom.wrt_log('Debug is active');

  for key in anom.JENKINS_ENV:
    if (len(anom.JENKINS_ENV[key]) > 0):
      anom.wrt_log('Jenkins Environment variable [%s]: %s' % (key, anom.JENKINS_ENV[key]));

# Check if s3cmd available and version for mime magic directive

  s3_cmd = "s3cmd --help | grep '\%s'" % MAGIC;                                                                               # look for MAGIC CLI option
  result = anom.exec_cmd(s3_cmd);
  if ((result[0] <= 1) and (len(result[1][1]) < 1)):
    if MAGIC not in (result[1][0]):
      magic = anom.NULCHR;
    else:
      magic = MAGIC;
    anom.wrt_log('s3cmd mime status %s' % magic);
  else:
    anom.err_exit('Error[%s] executing %s %s' % (result[0], s3_cmd, '%s%s' % (result[1][0], result[1][1])));                  # s3cmd most likely not installed or in path

# Get access keys for S3

  k = anom.exec_cmd(ACCESS_KEY_CMD);
  if k[0] == 0:
    ACCESS_KEY = k[1][0].rstrip(anom.nl);
    anom.wrt_log('Access Key Obtained');
  else:
    anom.err_exit('Error [%d]: Getting %s: %s' % (k[0], ACCESS_KEY_ENV, k[1]));

  k = anom.exec_cmd(SECRET_KEY_CMD);
  if k[0] == 0:
    SECRET_KEY = k[1][0].rstrip(anom.nl);
    anom.wrt_log('Secret Key Obtained');
  else:
    anom.err_exit('Error [%d]: Getting %s: %s' % (k[0], SECRET_KEY_ENV, k[1]));

# Set environment Variables for keys

  os.environ[ACCESS_KEY_ENV] = ACCESS_KEY;
  anom.wrt_log('Access Key Environment set');
  os.environ[SECRET_KEY_ENV] = SECRET_KEY;
  anom.wrt_log('Secret Key Environment set');

# Store keys to obfuscate

  OBFUSCATE_KEYS.extend([ACCESS_KEY, SECRET_KEY]);

# Get list of files to perform ETL

  for i in range(0, len(FILE_PROC_LIST)):

# Set dateformat for copy is set in json file

    DATEFORMAT = DATEFORMAT1;
    TIMEFORMAT = TIMEFORMAT1;

    if TABLE_STRUCT is not None:
      if TABLE_STRUCT.has_key(REDSHIFT_TABLE_LIST[i]): FILE_STRUCT = TABLE_STRUCT[REDSHIFT_TABLE_LIST[i]];
      if FILE_STRUCT.has_key('DateFormat') : DATEFORMAT = FILE_STRUCT['DateFormat'];
      if FILE_STRUCT.has_key('TimeFormat') : TIMEFORMAT = FILE_STRUCT['TimeFormat'];

    S3_GZ_FILES = anom.json_obj['GZ_FILES'];

# Copy data to redshift

    for gz_file in S3_GZ_FILES[i]:

# Move data into redshift database staging table (truncate prior to copy to ensure table is empty)

      rs_sql = \
      [
        "TRUNCATE TABLE %s%s%s;" % (SCHEMA_NAME, STAGING, REDSHIFT_TABLE_LIST[i]),
        "COPY " + \
        "%s%s%s FROM '%s%s%s' " % (SCHEMA_NAME, STAGING, REDSHIFT_TABLE_LIST[i], S3_BUCKET, S3_PATH, gz_file) + \
        "WITH CSV DELIMITER '%s' " % SQL_DEL + \
        "DATEFORMAT '%s' " % DATEFORMAT + \
        "TIMEFORMAT '%s' " % TIMEFORMAT + \
        '%s ' % TRUNCATE + \
        "EMPTYASNULL GZIP " + \
        "IGNOREHEADER %d " % anom.options.ignore_header + \
        "ACCEPTINVCHARS AS '^' " + \
        "MAXERROR AS %d " % ERROR_COUNT + \
        "CREDENTIALS AS 'aws_access_key_id=%s;aws_secret_access_key=%s';" % (ACCESS_KEY, SECRET_KEY),
        "SELECT COUNT(*) FROM %s%s%s;" % (SCHEMA_NAME, STAGING, REDSHIFT_TABLE_LIST[i]),
      ];

# Add staging sql to list of statements

      SQL_CMD_LIST = staging_sql();
      try:
        for sql_cmd in SQL_CMD_LIST[i]:
          rs_sql.append(sql_cmd.format(REDSHIFT_TABLE_LIST[i]));
      except Exception as e:
        anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
        anom.wrt_log(anom.obfuscate('SQL Command {0}\n{1}'.format(sql_cmd, anom.Errmsg), OBFUSCATE_KEYS));
        anom.err_exit(1);

# Cleanup staging table sql commands to reduce load in redshift

      tr_sql = \
      [
        "TRUNCATE TABLE %s%s%s;" % (SCHEMA_NAME, STAGING, REDSHIFT_TABLE_LIST[i]),
      ];

# Add cleanup sql commands to list of statements

      for sql_cmd in tr_sql:
        rs_sql.append(sql_cmd);

# Build redshift SQL commands for execution

      for sql_cmd in rs_sql:
        rs_cmd = 'psql %s -c "%s"' % (PSQLARGS, sql_cmd);

        result = anom.exec_cmd(rs_cmd);
        if result[0] == 0:
          anom.wrt_log('%s\n%s' % (anom.obfuscate(rs_cmd, OBFUSCATE_KEYS), '%s%s' % (result[1][0], result[1][1])));
        else:
          anom.wrt_log('Error[%s] executing %s %s\nCheck stl_load_errors for possible errors as well' % (result[0], anom.obfuscate(rs_cmd, OBFUSCATE_KEYS), result[1]));
          anom.err_exit(result[0]);

# Delete the gz source file

      anom.del_fil(gz_file);

# Vacuum all tables affected by this script

    rs_sql = \
    [
      "VACUUM " + \
      "%s%s%s;" % (SCHEMA_NAME, STAGING, REDSHIFT_TABLE_LIST[i]),
      "VACUUM " + \
      "%s%s;" % (SCHEMA_NAME, REDSHIFT_TABLE_LIST[i]),
    ];

    for sql_cmd in rs_sql:
      rs_cmd = 'psql %s -c "%s"' % (PSQLARGS, sql_cmd);

      result = anom.exec_cmd(rs_cmd);
      if result[0] == 0:
        anom.wrt_log('%s\n%s' % (rs_cmd, '%s%s' % (result[1][0], result[1][1])));
      else:
        anom.wrt_log('Error[%s] executing %s %s' % (result[0], rs_cmd, '%s%s' % (result[1][0], result[1][1])));               # non critical, don't exit

  for sql_cmd in anom.Monitor_list:
    rs_cmd = 'psql %s -c "%s"' % (PSQLARGS, sql_cmd);

    result = anom.exec_cmd(rs_cmd);
    if result[0] == 0:
      anom.wrt_log('%s\n%s' % (rs_cmd, '%s%s' % (result[1][0], result[1][1])));
    else:
      anom.wrt_log('Error[%s] executing %s %s' % (result[0], rs_cmd, '%s%s' % (result[1][0], result[1][1])));                 # non critical, don't exit

# write missing file list to log file

  if len(anom.Missing) > 0 :
    anom.wrt_log('Missing input file types %s' % anom.Missing);

    MSG_BODY = '';

    for MISSING in anom.Missing:
      MSG_BODY += 'File missing: %s\n' % MISSING;

    if not (anom.snd_eml(FR_ADDR, TO_ADDR, SMTP_SVR, SMTP_PRT, SMTP_USER, SMTP_PWD, SMTP_SBJ, MSG_BODY)):                     # send email to development team
      anom.wrt_log('%s %s' % (anom.Errmsg, anom.Missing));
      anom.err_exit('Missing File email not sent');                                                                           # force build fail in jenkins

  anom.cls_log();                                                                                                             # close log file
  os.chdir(anom.Orgdir);                                                                                                      # change to original directory

  return;

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Script entrant point
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

if (__name__ == '__main__'): main();
#  try:
#    main();
#  except Exception as e:
#    for sql_cmd in anom.Monitor_list:
#      rs_cmd = 'psql %s -c "%s"' % (PSQLARGS, sql_cmd);
#
#      result = anom.exec_cmd(rs_cmd);
#      if result[0] == 0:
#        anom.wrt_log('%s\n%s' % (rs_cmd, '%s%s' % (result[1][0], result[1][1])));
#      else:
#        anom.wrt_log('Error[%s] executing %s %s' % (result[0], rs_cmd, '%s%s' % (result[1][0], result[1][1])));                 # non critical, don't exit
#
#    msg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e.args), inspect.stack()[0][3], __name__);
#    anom.wrt_log("Globally Trapped Error: %s" % msg);
#    anom.err_exit(msg);

sys.exit();
