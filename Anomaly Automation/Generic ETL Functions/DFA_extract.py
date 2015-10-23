#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# DFA_extract.py
#
#   Functions file for ec2_s3_redshift.py ETL script
#
# Assumptions:
#
# 1. You know what you are doing.
# 2. If you don't know what you are doing, you have the sense to ask someone who can assist you.
# 3. If you still don't know, you will LEAVE IT ALONE.
# 
# Copyright (c) 2015 Anomaly
# Author Leo Li
#----------------------------------------
#     I M P O R T A N T  N O T E S
#
#      Read the above Assumptions
#----------------------------------------

import shutil;
import glob;
import os;
#*# Machine alterable code below this line.

BASE_DIR = '/mnt/data/data-collection/DFA-download/';
DEST_DIR = '%s/' % os.environ['WORKSPACE'];



#  'match_tables/',                                                                                                            # future expansion

def extract(f_list, d_list, jobname):
  
  for i in range(0, len(f_list)):
      
    source_dir = '%s%s' % (BASE_DIR, d_list[i]);                                                                                      # construct full source directory
    try:
      file_list = glob.glob('%s%s%s' % (source_dir, anom.WILDCARD, f_list[i]));
      print '%s From glob' % file_list;
      for f in file_list:
        shutil.copy2(f, DEST_DIR);                                                                                            # copy files
        anom.wrt_log('%s copied to %s' % (f, DEST_DIR));
    except IOError as e:
      anom.Errmsg = "{0}: {1} {2} {3}".format(type(e).__name__, e.errno, e.strerror, e.filename);
      return False;
      
  return True;


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# End of extract code
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
