#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# summer_anom_sizmek_Extract.py
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

# Get command line parameters

  start_date = anom.options.start_date;
  end_date = anom.options.end_date;

# Set time now

  anom.curdat();                                                                                                              # initialise date
  date_today = anom.datstr(Revers=True, Sep="-");                                                                             # get date as YYYY-MM-DD
  date_yesterday = (parse(date_today) - timedelta(days=1)).strftime("%Y-%m-%d");                                              # set yesterday's date
  datmrk = False;
  rolling_days = anom.options.rolling_days;                                                                                   # get rolling business logic

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Date Business Rules
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 1. --start-date only = getftp pull from that date to yesterday
# 2. --end-date only = getftp pull that date only
# 3. --start-date and --end-date = getftp pull for that date range
# 4. no dates specified = use date marker file and getftp pull that date and back for rolling number of days

# Check for date marker file

  if (len(start_date) + len(end_date)) < 1:
    Date_marker = glob.glob('%s%s' % (anom.WILDCARD, anom.MRKEXT));                                                           # check for possible end date
    if (len(Date_marker) < 1):
      end_date = date_yesterday;                                                                                              # force end date to today
      f_date = (parse(end_date)).strftime('%Y%m%d');
      Date_marker = ['%s%s' % (f_date, anom.MRKEXT)];
      open(Date_marker[0], "wb").close();
      anom.wrt_log('Date marker set to [%s]' % os.path.split(Date_marker[0])[1]);
    else:
      end_date = (parse(os.path.splitext(os.path.basename(Date_marker[0]))[0])).strftime("%Y-%m-%d");                         # get end date from date marker
    start_date = (parse(end_date)).strftime("%Y-%m-%d");                                                                      # set start date
    datmrk = True;
  else:
    if len(end_date) < 1:
      end_date = date_yesterday;
    elif len(start_date) < 1:
      start_date = end_date;

# get ftp host details

  FTP_SOURCE = anom.json_obj['Source'];
  FTP_HOST = FTP_SOURCE['Host'];
  FTP_USER = FTP_SOURCE['User'];
  FTP_PASS = FTP_SOURCE['Password'];
  FTP_DIR = FTP_SOURCE['Directory'];
  FTP_TEMPLATE = anom.json_obj['Template'];

# Check template length matches directory length

  if len(FTP_DIR) != len(FTP_TEMPLATE):
    anom.Errmsg = 'Template list length does not match Directory list length in %s' % anom.options.json_file;
    return False;

# Build a list of files for date range

  for i in range(0, len(FTP_TEMPLATE)):

    try:
      file_list = anom.datlst(start_date, end_date, template=FTP_TEMPLATE[i][0]);                                             # build a list of files to extract
      anom.wrt_log('Files to download %s' % str(file_list));
      anom.getftp(FTP_HOST, FTP_USER, FTP_PASS, file_list, ld=anom.Curpth, rd=FTP_DIR[i]);

    except Exception as e:
      anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return False;

# Determine type of file returned from ftp source

    for file in anom.Ftp_fillst:
      anom.Ziplist = [];                                                                                                       # initialise zip file list
      Cmplst = anom.cmp_typ(file);                                                                                            # get file type from file content
      Filtyp = Cmplst[0];
      anom.wrt_log('File %s is type %s' % (file, Cmplst[1]));

# Un-zip if necessary

      if (Filtyp == anom.ZIPEXT):
        try:
          anom.unzip_file(file, multi=True);                                                                                  # unzip
          anom.wrt_log('unzipped %s to %s' % (file, str(anom.Ziplist)));
          anom.del_fil(file);                                                                                                 # delete .ZIP file
          for f in f_list[FTP_TEMPLATE[i][1]:FTP_TEMPLATE[i][2]]:                                                     # check to see if any files missing from zip list
            if f not in ','.join(anom.Ziplist):
              anom.Missing.append('%s missing from %s' % (f, file));
        except:
          anom.wrt_log(anom.Errmsg);
          anom.Missing.append('Could not unzip %s' % file);
          continue;

# Un-gzip if necessary

      if (Filtyp == anom.GZFEXT):
        try:
          anom.file_ungz(file);                                                                                               # gzip
          anom.wrt_log('un-gzipped %s' % file);
        except:
          anom.wrt_log(anom.Errmsg);
          anom.Missing.append('Could not gunzip %s' % file);
          continue;

# Clean up redundant files from workspace

  r_list = [];
  if anom.json_obj.has_key('Redundant'): r_list = anom.json_obj['Redundant'];
  for r in range(0, len(r_list)):
    r_files = glob.glob(r_list[r] % anom.WILDCARD);
    for file in r_files:
      anom.del_fil(file);

  if datmrk:
    f_del = Date_marker[0];
    os.remove(f_del);
    f_date = (parse(end_date) + timedelta(1)).strftime('%Y%m%d');
    Date_marker = ['%s%s' % (f_date, anom.MRKEXT)];
    open(Date_marker[0], "wb").close();
    anom.wrt_log('Date markers changed from [%s] to [%s]' % (os.path.split(f_del)[1], os.path.split(Date_marker[0])[1]));

  return True;
