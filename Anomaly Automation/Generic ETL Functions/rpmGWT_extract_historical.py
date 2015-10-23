#------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# rpmGWT-extract.py
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

import glob;
import os;
import shutil;

def extract(f_list, d_list=[], jobname=''):

# Get a list of paths (files or directories, we don't care at the moment)

  source_dir = d_list[0];
  paths = glob.glob('%s/*' % source_dir);
  source_paths = [];

# Get a list of directories from the path list

  for p in paths:
    if (os.path.isdir(p)):
      source_paths.append('%s/' % p);

  path_list = []
  for p in source_paths:
    paths = glob.glob('%s/*' % p)
    pa = [];
    for a in paths:
      if os.path.isdir(a):
        pa.append('%s/' % a)
    path_list.extend(pa)

  path_list.sort();
  source_paths = path_list

# Move the files from download area to workspace

  try:
    if len(source_paths) > 0:
      for source_dir in source_paths:
        check_path = '%sprocessing' % source_dir;
        if os.path.exists(check_path):
          continue;                                                                                                           # skip this folder as processing is underway
        for file_name in f_list:
          Ziplist = [];
          for file in glob.glob('%s%s%s%s%s' % (source_dir, anom.WILDCARD, file_name, anom.WILDCARD, anom.CSVEXT)):
            Ziplist.append(file);
          if (not anom.zip_file(Ziplist, '%s%s_%s%s' % (source_dir, anom.fildtm(), file_name, anom.ZIPEXT))):
            return False;
          for file in Ziplist:
            shutil.move(file, '.');
  except IOError as e:
    anom.Errmsg = "{0}: {1} {2} {3}".format(type(e).__name__, e.errno, e.strerror,  e.filename);
    return False;

  return True;

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# End of extract code
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
