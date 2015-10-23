#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# DFA_API_Extract.py
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
import csv;

def extract(f_list, d_list, jobname):

  anom.wrt_log(anom.options, anom.inputs);

# Get command line parameters

  no_download = anom.options.setup;                                                                                           # just get credentials

  start_date = anom.options.start_date;
  end_date = anom.options.end_date;

# Set time now

  anom.curdat();                                                                                                              # initialise date
  date_today = anom.datstr(Revers=True, Sep="-");                                                                             # get date as YYYY-MM-DD
  date_range = '';
  datmrk = False;
  rolling_days = anom.options.rolling_days;                                                                                   # get rolling business logic

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Date Business Rules
#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# 1. --start-date only = API pull from that date to current
# 2. --end-date only = API pull that date only
# 3. --start-date and --end-date = API pull for that date range
# 4. no dates specified = today

# Check for date marker file

  if (len(start_date) + len(end_date)) < 1:
    end_date = date_today;
    start_date = end_date;
  else:
    if len(end_date) < 1:
      end_date = date_today;
    if len(start_date) < 1:
      start_date = end_date;

  date_range = anom.datlst(start_date, end_date);                                                                             # set date template for java script
  anom.wrt_log(str(date_range));

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# This java API will be replaced with python API

  if (end_date == date_today):
    if (not no_download):
      java_cmd = 'java -jar DFA.jar';

      return_code, response = anom.exec_cmd(java_cmd, logging=True);                                                              # logging true so we can collect output from java script

      if (return_code != 0):
        anom.Errmsg = 'Error [%d]: Getting %s: %s' % (return_code, java_cmd, response);
        return False;
      else:
        anom.wrt_log(str(response));

#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Merge resultant files into a consolidated file

  try:
    for d in date_range:
      try:
        os.stat(d);                                                                                                           # exists?
        anom.wrt_log('Getting files for %s' % parse(d).strftime("%Y-%m-%d"));
      except:
        Err = 'DIRECTORY %s does not exist' % d;
        anom.wrt_log(Err);                                                                                                    # log the missing directory
        anom.Missing.append(Err);
        continue;
      zip_files = glob.glob('%s/%s/*%s' % (anom.CURDIR, d, anom.CSVEXT));                                                     # get list of downloaded files
      if len(zip_files) < 1: continue;                                                                                        # no files found
      if not anom.zip_file(zip_files, '%s%s' % (d, anom.ZIPEXT), mode='w'): return False;
      for f in f_list:
        for i in ['Booked', 'Lookup']:
          flag = i in f;
          if flag: break;                                                                                                     # found a match the break out of loop
        files = glob.glob('%s/%s/*%s*%s' % (anom.CURDIR, d, f, anom.CSVEXT));                                                 # get list of downloaded files
        if len(files) < 1: continue;                                                                                          # no files found
        files.sort();
        anom.wrt_log(str(files));
        for file in files:
          if 'full' in file:
            files.remove(file);
            anom.wrt_log('Removed full file [%s] from list' % file);
        if len(files) < 1: continue;                                                                                          # no files found
        s = (parse(d) - timedelta(1)).strftime("%Y-%m-%d");
        e = parse(d).strftime("%Y-%m-%d");
        with open('%s_%s_%s_full%s' % (f, s, e, anom.CSVEXT), 'wb') as outfile:
          anom.wrt_log('Created file [%s_%s_%s_full%s]' % (f, s, e, anom.CSVEXT));
          for i in range(0, len(files)):                                                                                      # keep the header from first file
            with open(files[i], 'rb') as fn:
              basefile = os.path.split(files[i])[1];                                                                          # get base filename
              anom.strtok(basefile, '_', False);                                                                              # get profile id from file name
              profile_id = anom.lstval[0];                                                                                    # store it
              reader = csv.reader(fn);                                                                                        # create reader object
              writer = csv.writer(outfile, lineterminator='\n');                                                              # create writer object
              rows = [];                                                                                                      # initialize rows list
              row = next(reader);                                                                                             # get the first record
              if (i < 1):
                if flag: row.append('ProfileId');                                                                             # add column header if first file
                rows.append(row);                                                                                             # store to rows list
              for row in reader:
                if flag: row.append(profile_id);                                                                              # add profileid to record
                rows.append(row);                                                                                             # store record to list
            writer.writerows(rows);                                                                                           # write list to output file
            msg = 'Merged file [%s]' % files[i];
            if flag: msg = 'Converted and %s' % msg;
            anom.wrt_log(msg);
        for file in files:
          os.remove(file);
          anom.wrt_log('File [%s] removed' % file);
  except Exception as e:
    anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
    return False;

  return True;
