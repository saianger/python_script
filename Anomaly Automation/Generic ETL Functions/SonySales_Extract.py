#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# SonySales_Extract.py
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

import inspect;
import os;

def extract(f_list, d_list, jobname):

  anom.curdtm();
  start_tms = anom.tmsval;                                                                                                    # capture timestamp for monitoring

# Create sftp connection

  FTP_SOURCE = anom.json_obj['Source'];
  FTP_HOST = FTP_SOURCE['Host'];
  FTP_USER = FTP_SOURCE['User'];
  FTP_PASS = FTP_SOURCE['Password'];
  FTP_DIR = FTP_SOURCE['Directory'];
  FTP_SSH = '';
  if FTP_SOURCE.has_key('SSH_Path'): FTP_SSH = FTP_SOURCE['SSH_Path'];
  Source = '%s%s%s' % (FTP_USER, FTP_HOST, FTP_DIR);
  volume = {};

  Sftp = anom.mksftp(FTP_HOST, FTP_USER, FTP_PASS, ssh=FTP_SSH);

  if Sftp is None: return False;

# Change to remote source directory

  Sftp.cwd(FTP_DIR);

# Check that we moved there

  if (str(Sftp.getcwd()) != FTP_DIR):
    anom.Errmsg = 'Cannot change to remote sftp directory %s' % FTP_DIR;
    return False;

# Get list of files from ftp source to local storage

  src_list = Sftp.listdir();
  if len(src_list) < 1:
    anom.wrt_log('No files on sftp server');
  else:
    anom.wrt_log(str(src_list));

# get the files to local storage

  for src_file in src_list:

    try:
      file = str(src_file)
      Sftp.get(file);
      anom.wrt_log('Fetched %s' % file);
      Sftp.remove(file);
      anom.wrt_log('Removed via sftp %s' % file);
      key = '%s%s' % (Source, file);
      volume[key] = [os.path.getsize(file), Source, file, 0, 0];                                                               # capture data size for monitoring
    except Exception as e:
      anom.Errmsg = "{0}: {1} function:{2} module:{3}".format(type(e).__name__, str(e), inspect.stack()[0][3], __name__);
      return False;

# Close sftp client

  Sftp.close();

# Store data volume

  if (len(volume) > 0): anom.monitor(start_tms, __name__, anom.options.schema, 'Data', volume);

  return True;

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# End of extract code
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
