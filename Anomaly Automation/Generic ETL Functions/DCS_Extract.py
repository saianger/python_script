#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# DCS_Extract.py
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
import DCS_API;

def extract(f_list, d_list, jobname):

  anom.wrt_log(anom.options, anom.inputs);

# Get command line parameters

  no_download = anom.options.setup;                                                                                           # just get credentials

  start_date = anom.options.start_date;
  end_date = anom.options.end_date;

# Set time now

  anom.curdat();                                                                                                              # initialise date
  date_today = anom.datstr(Revers=True, Sep="-");                                                                             # get date as YYYY-MM-DD
  DAT_DIR = anom.datstr(Revers=True, Sep="");                                                                                 # get date as YYYYMMDD
  date_range = '';
  datmrk = False;
  rolling_days = anom.options.rolling_days;                                                                                   # get rolling business logic

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Date Business Rules
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 1. --start-date only = API pull only that date
# 2. --end-date only = API pull that date and back for rolling number of days
# 3. --start-date and --end-date = API pull for that date range
# 4. no dates specified = use date marker file amd API pull that date and back for rolling number of days

# Check for date marker file

  if (len(start_date) + len(end_date)) < 1:
    Date_marker = glob.glob('%s%s' % (anom.WILDCARD, anom.MRKEXT));                                                           # check for possible end date
    if (len(Date_marker) < 1):
      end_date = date_today;                                                                                                  # force end date to today
      f_date = (parse(end_date)).strftime('%Y%m%d');
      Date_marker = ['%s%s' % (f_date, anom.MRKEXT)];
      open(Date_marker[0], "wb").close();
      anom.wrt_log('Date marker set to [%s]' % os.path.split(Date_marker[0])[1]);
    else:
      end_date = (parse(os.path.splitext(os.path.basename(Date_marker[0]))[0])).strftime("%Y-%m-%d");                         # get end date from date marker
    start_date = (parse(end_date) - timedelta(days=rolling_days)).strftime("%Y-%m-%d");                                       # set start date
    datmrk = True;
  else:
    if len(end_date) < 1:
      end_date = start_date;
    else:
      start_date = (parse(end_date) - timedelta(days=rolling_days)).strftime("%Y-%m-%d");                                     # set end date to be rolling number of days prior

  date_range = ' %s %s' % (start_date, end_date);                                                                             # set date template for java script

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# This java API will be replaced with python API

#  if (not no_download):
#    java_cmd = 'java -jar DCS2.jar%s' % date_range;

#    return_code, response = anom.exec_cmd(java_cmd, logging=True);                                                              # logging true so we can collect output from java script

#    if (return_code != 0):
#      anom.Errmsg = 'Error [%d]: Getting %s: %s' % (return_code, java_cmd, response);
#      return False;
#    else:
#      anom.wrt_log(str(response));

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  DCS_API.main(start_date, end_date)
# Merge resultant files into a consolidated file

  try:
    for f in f_list:
      files = glob.glob('%s/%s/*%s*%s' % (anom.CURDIR, DAT_DIR, f, anom.CSVEXT));
      if len(files) < 1:
        anom.Errmsg =  "No files found: function:{0} module:{1} Line:{2}".format(inspect.stack()[0][3], __name__, inspect.stack()[0][2]);
        return False;
      files.sort();
      anom.wrt_log(str(files));
      for file in files:
        if 'full' in file:
          files.remove(file);
          anom.wrt_log('Removed full file [%s] from list' % file);
      with open('%s%s_%s_full%s' % (f, start_date, end_date, anom.CSVEXT), 'wb') as outfile:
        anom.wrt_log('Created file [%s_%s_%s_full%s]' % (f, start_date, end_date, anom.CSVEXT));
        for i in range(0, len(files)):                                                                                        # keep the header from first file
          with open(files[i], 'rb') as fn:
            if (i > 0): fn.readline();                                                                                        # drop header record
            outfile.write(fn.read());
            anom.wrt_log('Merged file [%s]' % file);
      for file in files:
        os.remove(file);
        anom.wrt_log('File [%s] removed' % file);
    os.rmdir('%s/%s' % (anom.CURDIR, DAT_DIR));
  except Exception as e:
    anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
    return False;

  if datmrk:
    f_del = Date_marker[0];
    os.remove(f_del);
    f_date = (parse(end_date) + timedelta(1)).strftime('%Y%m%d');
    Date_marker = ['%s%s' % (f_date, anom.MRKEXT)];
    open(Date_marker[0], "wb").close();
    anom.wrt_log('Date markers changed from [%s] to [%s]' % (os.path.split(f_del)[1], os.path.split(Date_marker[0])[1]));

  return True;
